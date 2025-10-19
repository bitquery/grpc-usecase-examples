const grpc = require("@grpc/grpc-js");
const fs = require("fs");
const yaml = require("js-yaml");
const bs58 = require("bs58");
const { loadPackageDefination } = require("bitquery-corecast-proto");

// Global state
let config = null;
let client = null;
let metadata = null;
let currentStream = null;
let isReloading = false;

// Performance tracking
let tradeCount = 0;
let startTime = Date.now();
let lastReportTime = Date.now();
let tradesInLastInterval = 0;
let performanceTimer = null;

// Auto-reconnect tracking
let reconnectAttempts = 0;
let reconnectTimer = null;
let isIntentionalDisconnect = false;
const MAX_RECONNECT_DELAY = 30000; // 30 seconds max
const INITIAL_RECONNECT_DELAY = 1000; // 1 second initial

// Helper function to convert bytes to base58
function toBase58(bytes) {
  if (!bytes || bytes.length === 0) return "undefined";
  try {
    return bs58.encode(bytes);
  } catch (error) {
    return "invalid_address";
  }
}

// Helper function to calculate trade price
function calculateTradePrice(buyAmount, sellAmount) {
  const buy = parseFloat(buyAmount);
  const sell = parseFloat(sellAmount);

  if (!buy || !sell || buy === 0 || sell === 0) {
    return {
      pricePerBuyToken: null,
      pricePerSellToken: null,
      valid: false,
    };
  }

  // Price per buy token = how much sell token per 1 buy token
  const pricePerBuyToken = sell / buy;

  // Price per sell token = how much buy token per 1 sell token
  const pricePerSellToken = buy / sell;

  return {
    pricePerBuyToken,
    pricePerSellToken,
    valid: true,
  };
}

// Helper function to format large numbers
function formatNumber(num) {
  if (num === null || num === undefined) return "N/A";

  // Use scientific notation for very large or very small numbers
  if (Math.abs(num) >= 1e9 || (Math.abs(num) < 1e-6 && Math.abs(num) > 0)) {
    return num.toExponential(6);
  }

  // Otherwise use fixed decimal places
  return num.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 8,
  });
}

// Load proto files using the package's helper function
const packageDefinition = loadPackageDefination();
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const solanaCorecast = protoDescriptor.solana_corecast;

// Load configuration from file
function loadConfig() {
  try {
    const newConfig = yaml.load(fs.readFileSync("./config.yaml", "utf8"));
    // console.log("Configuration loaded successfully");
    return newConfig;
  } catch (error) {
    console.error("Failed to load configuration:", error.message);
    return null;
  }
}

// Initialize gRPC client and metadata
function initializeClient() {
  if (!config) {
    throw new Error("Configuration not loaded");
  }

  client = new solanaCorecast.CoreCast(
    config.server.address,
    grpc.credentials.createSsl()
  );

  metadata = new grpc.Metadata();
  metadata.add("authorization", config.server.authorization);

  // console.log("gRPC client initialized");
}

// Create request based on configuration
function createRequest() {
  const request = {};

  if (config.filters.programs && config.filters.programs.length > 0) {
    request.program = {
      addresses: config.filters.programs,
    };
  }

  if (config.filters.pool && config.filters.pool.length > 0) {
    request.pool = {
      addresses: config.filters.pool,
    };
  }

  if (config.filters.traders && config.filters.traders.length > 0) {
    request.trader = {
      addresses: config.filters.traders,
    };
  }

  if (config.filters.signers && config.filters.signers.length > 0) {
    request.signer = {
      addresses: config.filters.signers,
    };
  }

  return request;
}

// Stop current stream
function stopStream(intentional = false) {
  isIntentionalDisconnect = intentional;
  
  if (currentStream) {
    try {
      currentStream.cancel();
      // console.log("Stream stopped");
    } catch (error) {
      console.error("Error stopping stream:", error.message);
    }
    currentStream = null;
  }

  // Stop performance reporting
  if (performanceTimer) {
    clearInterval(performanceTimer);
    performanceTimer = null;
  }
  
  // Clear any pending reconnect timers
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

// Calculate reconnect delay with exponential backoff
function getReconnectDelay() {
  const delay = Math.min(
    INITIAL_RECONNECT_DELAY * Math.pow(2, reconnectAttempts),
    MAX_RECONNECT_DELAY
  );
  return delay;
}

// Attempt to reconnect
function attemptReconnect() {
  if (isIntentionalDisconnect || isReloading) {
    return;
  }

  reconnectAttempts++;
  const delay = getReconnectDelay();
  
  console.log(
    `\nReconnecting in ${(delay / 1000).toFixed(1)}s (attempt ${reconnectAttempts})...`
  );

  reconnectTimer = setTimeout(() => {
    try {
      console.log("Attempting to reconnect...");
      startStream();
    } catch (error) {
      console.error("Reconnection failed:", error.message);
      attemptReconnect(); // Try again
    }
  }, delay);
}

// Print performance statistics
function printPerformanceStats() {
  const now = Date.now();
  const totalElapsed = (now - startTime) / 1000; // seconds
  const intervalElapsed = (now - lastReportTime) / 1000; // seconds

  const avgTradesPerSecond = (tradeCount / totalElapsed).toFixed(2);
  const intervalTradesPerSecond = (
    tradesInLastInterval / intervalElapsed
  ).toFixed(2);

  console.log("\n" + "=".repeat(60));
  console.log("PERFORMANCE STATISTICS");
  console.log("=".repeat(60));
  console.log(`  Total Trades Parsed: ${tradeCount.toLocaleString()}`);
  console.log(
    `  Total Runtime: ${Math.floor(totalElapsed / 60)}m ${Math.floor(
      totalElapsed % 60
    )}s`
  );
  console.log(`  Average Rate: ${avgTradesPerSecond} trades/sec`);
  console.log(
    `  Current Rate: ${intervalTradesPerSecond} trades/sec (last ${Math.floor(
      intervalElapsed
    )}s)`
  );
  console.log(`  Trades in Last Interval: ${tradesInLastInterval}`);
  console.log("=".repeat(60) + "\n");

  // Reset interval counters
  lastReportTime = now;
  tradesInLastInterval = 0;
}

// Start performance reporting
function startPerformanceReporting(intervalSeconds = 30, resetCounters = true) {
  // Only reset counters on initial start, not on reconnect
  if (resetCounters) {
    tradeCount = 0;
    startTime = Date.now();
    lastReportTime = Date.now();
    tradesInLastInterval = 0;
  }

  // Clear existing timer if any
  if (performanceTimer) {
    clearInterval(performanceTimer);
  }

  // Start new timer
  performanceTimer = setInterval(() => {
    printPerformanceStats();
  }, intervalSeconds * 1000);

  // console.log(
  //   `Performance reporting enabled (every ${intervalSeconds} seconds)\n`
  // );
}

// Stream listener function
function startStream() {
  if (!client || !config) {
    throw new Error("Client not initialized");
  }

  // console.log("\nConnecting to CoreCast stream...");
  // console.log("   Server:", config.server.address);
  // console.log("   Stream type:", config.stream.type);
  // console.log("   Filters:", JSON.stringify(config.filters, null, 2));

  const request = createRequest();

  // Create stream for dex_trades only
  const stream = client.DexTrades(request, metadata);

  currentStream = stream;

  // Handle stream events
  stream.on("data", (message) => {
    // console.log("\n=== New Message ===");
    // console.log("Block Slot:", message.Block?.Slot);
    // console.log("Transaction Index:", message.Transaction?.Index);
    // console.log(
    //   "Transaction Signature:",
    //   toBase58(message.Transaction?.Signature)
    // );
    // console.log("Transaction Status:", message.Transaction?.Status);

    // Handle different message types
    if (message.Trade) {
      // Increment trade counters
      tradeCount++;
      tradesInLastInterval++;

      // Uncomment below lines to see the trade event details

      // console.log("Trade Event:");
      // console.log("  Instruction Index:", message.Trade.InstructionIndex);
      // console.log(
      //   "  DEX Program:",
      //   toBase58(message.Trade.Dex?.ProgramAddress)
      // );
      // console.log("  Protocol:", message.Trade.Dex?.ProtocolName);
      // console.log("  Market:", toBase58(message.Trade.Market?.MarketAddress));

      // // Buy side with token info
      // const buyTokenName = message.Trade.Buy?.Currency?.Name || "Unknown";
      // const buyTokenSymbol = message.Trade.Buy?.Currency?.Symbol || "N/A";
      // const buyTokenAddress = toBase58(message.Trade.Buy?.Currency?.Address);
      // console.log("  Buy Token Address:", buyTokenAddress);
      // console.log("  Buy Amount:", message.Trade.Buy?.Amount);
      // console.log(
      //   "  Buy Account (Trader):",
      //   toBase58(message.Trade.Buy?.Account?.Address)
      // );

      // // Sell side with token info
      // const sellTokenName = message.Trade.Sell?.Currency?.Name || "Unknown";
      // const sellTokenSymbol = message.Trade.Sell?.Currency?.Symbol || "N/A";
      // const sellTokenAddress = toBase58(message.Trade.Sell?.Currency?.Address);
      // console.log("  Sell Token Address:", sellTokenAddress);
      // console.log("  Sell Amount:", message.Trade.Sell?.Amount);
      // console.log(
      //   "  Sell Account (Trader):",
      //   toBase58(message.Trade.Sell?.Account?.Address)
      // );

      // // Calculate and display trade price
      // const price = calculateTradePrice(
      //   message.Trade.Buy?.Amount,
      //   message.Trade.Sell?.Amount
      // );
      // if (price.valid) {
      //   console.log("  ┌─ PRICE CALCULATION ─────────────────────────────");
      //   console.log(
      //     `  │ Price: 1 ${buyTokenSymbol} = ${formatNumber(
      //       price.pricePerBuyToken
      //     )} ${sellTokenSymbol}`
      //   );
      //   console.log(
      //     `  │ Price: 1 ${sellTokenSymbol} = ${formatNumber(
      //       price.pricePerSellToken
      //     )} ${buyTokenSymbol}`
      //   );
      //   console.log("  └─────────────────────────────────────────────────");
      // } else {
      //   console.log("  Price: Unable to calculate (invalid amounts)");
      // }
    }
  });

  stream.on("error", (error) => {
    if (!isReloading && !isIntentionalDisconnect) {
      console.error("\nStream error:", error.details || error.message);
      console.error("Error code:", error.code);
      
      // Attempt to reconnect
      stopStream(false);
      attemptReconnect();
    }
  });

  stream.on("end", () => {
    if (!isReloading && !isIntentionalDisconnect) {
      console.log("\nStream ended unexpectedly");
      
      // Attempt to reconnect
      stopStream(false);
      attemptReconnect();
    }
  });

  stream.on("status", (status) => {
    if (!isReloading && status.code !== 0) {
      // console.log("Stream status:", status);
    }
  });

  // console.log("Stream connected and listening for data...\n");

  // Reset reconnect attempts on successful connection
  const isReconnecting = reconnectAttempts > 0;
  reconnectAttempts = 0;

  // Start performance reporting (every 30 seconds)
  // Don't reset counters if this is a reconnection
  if (!performanceTimer) {
    startPerformanceReporting(30, true); // Initial start - reset counters
  } else {
    // Reconnection - keep existing counters
    console.log("Reconnected! Trade counter preserved.");
  }
}

// Check if server configuration changed
function hasServerConfigChanged(oldConfig, newConfig) {
  return (
    oldConfig.server.address !== newConfig.server.address ||
    oldConfig.server.authorization !== newConfig.server.authorization ||
    oldConfig.server.insecure !== newConfig.server.insecure
  );
}

// Reload configuration and restart stream
function reloadAndRestart() {
  if (isReloading) {
    return; // Prevent concurrent reloads
  }

  isReloading = true;
  // console.log("\n Configuration changed, reloading...");

  // Load new configuration
  const newConfig = loadConfig();
  if (!newConfig) {
    console.error("Failed to reload configuration, keeping current settings");
    isReloading = false;
    return;
  }

  // Check if we need to reinitialize the client
  const needsNewClient = hasServerConfigChanged(config, newConfig);

  // Stop current stream (intentional disconnect)
  stopStream(true);

  // Update configuration
  config = newConfig;

  // Reinitialize client if server settings changed
  if (needsNewClient) {
    // console.log("Server configuration changed, reinitializing client...");
    try {
      initializeClient();
    } catch (error) {
      console.error("Failed to initialize client:", error.message);
      isReloading = false;
      return;
    }
  }

  // Start new stream
  try {
    startStream();
    isReloading = false;
  } catch (error) {
    console.error("Failed to start stream:", error.message);
    isReloading = false;
  }
}

// Handle process termination
process.on("SIGINT", () => {
  console.log("\n Shutting down gracefully...");
  stopStream(true);
  process.exit(0);
});

process.on("SIGTERM", () => {
  console.log("\n Shutting down gracefully...");
  stopStream(true);
  process.exit(0);
});

// Watch config file for changes
let watchTimeout = null;
fs.watch("./config.yaml", (eventType, filename) => {
  if (eventType === "change") {
    // Debounce multiple rapid file changes
    if (watchTimeout) {
      clearTimeout(watchTimeout);
    }
    watchTimeout = setTimeout(() => {
      reloadAndRestart();
      watchTimeout = null;
    }, 300); // Wait 300ms after last change
  }
});

// console.log(" Watching config.yaml for changes...");

// Initial startup
try {
  config = loadConfig();
  if (!config) {
    console.error("Failed to load configuration");
    process.exit(1);
  }

  initializeClient();
  startStream();
} catch (error) {
  console.error("Failed to start stream:", error);
  process.exit(1);
}
