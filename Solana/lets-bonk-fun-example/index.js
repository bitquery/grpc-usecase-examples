const grpc = require("@grpc/grpc-js");
const fs = require("fs");
const yaml = require("js-yaml");
const bs58 = require("bs58");
const { loadPackageDefination } = require("bitquery-corecast-proto");


// Performance optimization: Cache for base58 conversions
const base58Cache = new Map();
const MAX_CACHE_SIZE = 10000;

// Performance optimization: Batch console output
let logBuffer = [];
let logFlushInterval = null;
const LOG_FLUSH_INTERVAL_MS = 100;
const MAX_LOG_BUFFER_SIZE = 1000;

// Performance optimization: Message processing stats
let messageCount = 0;
let totalMessageSize = 0;
let transactionCount = 0;
let tradeCount = 0;
let filteredOutCount = 0;
let transferCount = 0;
let orderCount = 0;
let poolEventCount = 0;
let balanceUpdateCount = 0;
let lastStatsTime = Date.now();
const STATS_INTERVAL_MS = 30000;
let statsInterval = null;

// Load configuration
const config = yaml.load(fs.readFileSync("./config.yaml", "utf8"));

// Get trade filter setting (default to "alltrades")
const tradeFilter = config.trade_filter || "alltrades";

// Validate trade filter
if (!["alltrades", "buys", "sells"].includes(tradeFilter)) {
  console.error(
    `Invalid trade_filter: "${tradeFilter}". Must be one of: alltrades, buys, sells`
  );
  process.exit(1);
}

// Helper function to determine if a trade should be shown based on filter
function shouldShowTrade(trade) {
  // If no token filter is specified, show all trades
  if (!config.filters.tokens || config.filters.tokens.length === 0) {
    return true;
  }

  const buyMint = toBase58(trade.Buy?.Currency?.MintAddress);
  const sellMint = toBase58(trade.Sell?.Currency?.MintAddress);

  // Check if any filtered token is involved in the trade
  const tokenOnBuySide = config.filters.tokens.some(
    (token) => token === buyMint
  );
  const tokenOnSellSide = config.filters.tokens.some(
    (token) => token === sellMint
  );

  // Apply filter logic
  if (tradeFilter === "buys") {
    // Show only trades where the filtered token is being BOUGHT (on buy side)
    return tokenOnBuySide;
  } else if (tradeFilter === "sells") {
    // Show only trades where the filtered token is being SOLD (on sell side)
    return tokenOnSellSide;
  } else {
    // alltrades: show if token is on either side
    return tokenOnBuySide || tokenOnSellSide;
  }
}

// Optimized helper function to convert bytes to base58 with caching
function toBase58(bytes) {
  if (!bytes || bytes.length === 0) return "undefined";

  const cacheKey = Buffer.from(bytes).toString("hex");

  if (base58Cache.has(cacheKey)) {
    return base58Cache.get(cacheKey);
  }

  try {
    const result = bs58.encode(bytes);

    if (base58Cache.size >= MAX_CACHE_SIZE) {
      const firstKey = base58Cache.keys().next().value;
      base58Cache.delete(firstKey);
    }
    base58Cache.set(cacheKey, result);

    return result;
  } catch (error) {
    return "invalid_address";
  }
}

// Optimized logging system
function bufferedLog(message) {
  logBuffer.push(message);

  if (logBuffer.length >= MAX_LOG_BUFFER_SIZE) {
    flushLogs();
  }

  if (!logFlushInterval) {
    logFlushInterval = setInterval(() => {
      if (logBuffer.length > 0) {
        console.log(logBuffer.join("\n"));
        logBuffer = [];
      }
    }, LOG_FLUSH_INTERVAL_MS);
  }
}

function flushLogs() {
  if (logBuffer.length > 0) {
    console.log(logBuffer.join("\n"));
    logBuffer = [];
  }
}

function printStats() {
  const now = Date.now();
  const messagesPerSecond =
    messageCount > 0 ? (messageCount * 1000) / (now - lastStatsTime) : 0;
  const avgMessageSize =
    messageCount > 0 ? (totalMessageSize / messageCount).toFixed(2) : 0;
  const dataRateMBps =
    messageCount > 0
      ? totalMessageSize / (1024 * 1024) / ((now - lastStatsTime) / 1000)
      : 0;

  const statsMessage = [
    "\n=== Performance Stats ===",
    `Messages received: ${messageCount}`,
    `Messages shown: ${tradeCount}`,
    `Messages filtered out: ${filteredOutCount}`,
    `Rate: ${messagesPerSecond.toFixed(2)} msg/sec`,
    `Total data: ${(totalMessageSize / 1024).toFixed(2)} KB`,
    `Data rate: ${dataRateMBps.toFixed(2)} MB/sec`,
    `Avg message size: ${avgMessageSize} bytes`,
    "",
    "Message Types:",
    `  Transactions: ${transactionCount}`,
    `  Trades shown: ${tradeCount}`,
    `  Orders: ${orderCount}`,
    `  Pool Events: ${poolEventCount}`,
    `  Transfers: ${transferCount}`,
    `  Balance Updates: ${balanceUpdateCount}`,
    "",
    "System:",
    `  Cache size: ${base58Cache.size}`,
    `  Log buffer size: ${logBuffer.length}`,
    `  Memory usage: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(
      2
    )} MB`,
  ].join("\n");

  bufferedLog(statsMessage);

  messageCount = 0;
  totalMessageSize = 0;
  transactionCount = 0;
  tradeCount = 0;
  filteredOutCount = 0;
  transferCount = 0;
  orderCount = 0;
  poolEventCount = 0;
  balanceUpdateCount = 0;
  lastStatsTime = now;
}

// Load proto files using npm package
const packageDefinition = loadPackageDefination();

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const solanaCorecast = protoDescriptor.solana_corecast;

// Create gRPC client
const client = new solanaCorecast.CoreCast(
  config.server.address,
  config.server.insecure
    ? grpc.credentials.createInsecure()
    : grpc.credentials.createSsl(),
  {
    "grpc.keepalive_time_ms": 30000,
    "grpc.keepalive_timeout_ms": 5000,
    "grpc.keepalive_permit_without_calls": true,
    "grpc.http2.max_pings_without_data": 0,
    "grpc.http2.min_time_between_pings_ms": 10000,
    "grpc.http2.min_ping_interval_without_data_ms": 300000,
    "grpc.max_receive_message_length": 4 * 1024 * 1024,
    "grpc.max_send_message_length": 4 * 1024 * 1024,
    "grpc.enable_retries": 1,
    "grpc.max_connection_idle_ms": 30000,
    "grpc.max_connection_age_ms": 300000,
    "grpc.max_connection_age_grace_ms": 5000,
  }
);

// Create metadata
const metadata = new grpc.Metadata();
metadata.add("authorization", config.server.authorization);

// Create request with token filter support
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

  // Add token filter support
  if (config.filters.tokens && config.filters.tokens.length > 0) {
    request.token = {
      addresses: config.filters.tokens,
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

// Stream listener function with buy/sell filtering
function listenToStream() {
  console.log("=== Connecting to CoreCast Stream ===");
  console.log("Server:", config.server.address);
  console.log("Stream type:", config.stream.type);
  console.log("\n=== Active Filters ===");
  console.log("Trade Filter:", tradeFilter.toUpperCase());

  if (tradeFilter === "buys") {
    console.log("  → Showing only trades where filtered tokens are BOUGHT");
  } else if (tradeFilter === "sells") {
    console.log("  → Showing only trades where filtered tokens are SOLD");
  } else {
    console.log("  → Showing all trades involving filtered tokens");
  }

  if (config.filters.programs && config.filters.programs.length > 0) {
    console.log("\nPrograms:");
    config.filters.programs.forEach((p) => console.log(`  - ${p}`));
  }
  if (config.filters.tokens && config.filters.tokens.length > 0) {
    console.log("\nTokens:");
    config.filters.tokens.forEach((t) => console.log(`  - ${t}`));
  }
  if (config.filters.traders && config.filters.traders.length > 0) {
    console.log("\nTraders:");
    config.filters.traders.forEach((t) => console.log(`  - ${t}`));
  }
  if (config.filters.pool && config.filters.pool.length > 0) {
    console.log("\nPools:");
    config.filters.pool.forEach((p) => console.log(`  - ${p}`));
  }
  console.log("======================\n");

  statsInterval = setInterval(printStats, STATS_INTERVAL_MS);

  const request = createRequest();
  console.log("Request being sent:", JSON.stringify(request, null, 2));
  console.log("\nWaiting for trades...\n");

  let stream;
  switch (config.stream.type) {
    case "dex_trades":
      stream = client.DexTrades(request, metadata);
      break;
    case "dex_orders":
      stream = client.DexOrders(request, metadata);
      break;
    case "dex_pools":
      stream = client.DexPools(request, metadata);
      break;
    case "transactions":
      stream = client.Transactions(request, metadata);
      break;
    case "transfers":
      stream = client.Transfers(request, metadata);
      break;
    case "balances":
      stream = client.Balances(request, metadata);
      break;
    default:
      throw new Error(`Unsupported stream type: ${config.stream.type}`);
  }

  stream.on("data", (message) => {
    const receivedTimestamp = Date.now();
    messageCount++;

    const messageSize = Buffer.byteLength(JSON.stringify(message), "utf8");
    totalMessageSize += messageSize;

    if (message.Trade) {
      // Apply client-side trade filtering
      if (!shouldShowTrade(message.Trade)) {
        filteredOutCount++;
        return; // Skip this trade
      }

      tradeCount++;

      // Extract detailed account information
      const buyAccount = message.Trade.Buy?.Account;
      const sellAccount = message.Trade.Sell?.Account;
      const buyCurrency = message.Trade.Buy?.Currency;
      const sellCurrency = message.Trade.Sell?.Currency;

      // Determine trade direction label
      let tradeLabel = "Trade Event";
      if (tradeFilter === "buys") {
        tradeLabel = "🟢 BUY Trade";
      } else if (tradeFilter === "sells") {
        tradeLabel = "🔴 SELL Trade";
      }

      const logLines = [
        "\n" + "=".repeat(80),
        tradeLabel,
        "=".repeat(80),
        `Block Slot: ${message.Block?.Slot}`,
        `Timestamp: ${new Date(receivedTimestamp).toISOString()}`,
        `Instruction Index: ${message.Trade.InstructionIndex}`,
        "",
        "📍 DEX Info:",
        `  Program: ${toBase58(message.Trade.Dex?.ProgramAddress)}`,
        `  Protocol: ${message.Trade.Dex?.ProtocolName} (${message.Trade.Dex?.ProtocolFamily})`,
        "",
        "🏪 Market Info:",
        `  Address: ${toBase58(message.Trade.Market?.MarketAddress)}`,
        `  Base Currency: ${
          message.Trade.Market?.BaseCurrency?.Symbol || "N/A"
        }`,
        `  Quote Currency: ${
          message.Trade.Market?.QuoteCurrency?.Symbol || "N/A"
        }`,
        "",
        "💰 Buy Side:",
        `  Amount: ${message.Trade.Buy?.Amount}`,
        `  Currency: ${buyCurrency?.Symbol || "N/A"} (${
          buyCurrency?.Name || "Unknown"
        })`,
        `  Mint: ${toBase58(buyCurrency?.MintAddress)}`,
        `  Decimals: ${buyCurrency?.Decimals}`,
        `  Account: ${toBase58(buyAccount?.Address)}`,
        `  Is Signer: ${buyAccount?.IsSigner}`,
        `  Is Writable: ${buyAccount?.IsWritable}`,
        `  Order ID: ${toBase58(message.Trade.Buy?.Order?.OrderId)}`,
        "",
        "💸 Sell Side:",
        `  Amount: ${message.Trade.Sell?.Amount}`,
        `  Currency: ${sellCurrency?.Symbol || "N/A"} (${
          sellCurrency?.Name || "Unknown"
        })`,
        `  Mint: ${toBase58(sellCurrency?.MintAddress)}`,
        `  Decimals: ${sellCurrency?.Decimals}`,
        `  Account: ${toBase58(sellAccount?.Address)}`,
        `  Is Signer: ${sellAccount?.IsSigner}`,
        `  Is Writable: ${sellAccount?.IsWritable}`,
        `  Order ID: ${toBase58(message.Trade.Sell?.Order?.OrderId)}`,
        "",
        `💵 Fee: ${message.Trade.Fee}`,
        `👑 Royalty: ${message.Trade.Royalty}`,
        "=".repeat(80),
      ];

      bufferedLog(logLines.join("\n"));
    }

    if (message.Order) {
      orderCount++;
      const logLines = [
        "\n=== Order Event ===",
        `Block Slot: ${message.Block?.Slot}`,
        `Timestamp: ${new Date(receivedTimestamp).toISOString()}`,
        `Type: ${message.Order.Type}`,
        `Order ID: ${toBase58(message.Order.Order?.OrderId)}`,
        `Buy Side: ${message.Order.Order?.BuySide}`,
        `Limit Price: ${message.Order.Order?.LimitPrice}`,
        `Limit Amount: ${message.Order.Order?.LimitAmount}`,
        `Account: ${toBase58(message.Order.Order?.Account)}`,
        `Owner: ${toBase58(message.Order.Order?.Owner)}`,
        `Payer: ${toBase58(message.Order.Order?.Payer)}`,
        `Mint: ${toBase58(message.Order.Order?.Mint)}`,
      ];

      bufferedLog(logLines.join("\n"));
    }

    if (message.PoolEvent) {
      poolEventCount++;
      const logLines = [
        "\n=== Pool Event ===",
        `Block Slot: ${message.Block?.Slot}`,
        `Timestamp: ${new Date(receivedTimestamp).toISOString()}`,
        `Market: ${toBase58(message.PoolEvent.Market?.MarketAddress)}`,
        `Base Currency Change: ${message.PoolEvent.BaseCurrency?.ChangeAmount}`,
        `Base Currency Post Amount: ${message.PoolEvent.BaseCurrency?.PostAmount}`,
        `Quote Currency Change: ${message.PoolEvent.QuoteCurrency?.ChangeAmount}`,
        `Quote Currency Post Amount: ${message.PoolEvent.QuoteCurrency?.PostAmount}`,
      ];

      bufferedLog(logLines.join("\n"));
    }

    if (message.Transfer) {
      transferCount++;
      const logLines = [
        "\n=== Transfer Event ===",
        `Block Slot: ${message.Block?.Slot}`,
        `Timestamp: ${new Date(receivedTimestamp).toISOString()}`,
        `Amount: ${message.Transfer.Amount}`,
        `Sender: ${toBase58(message.Transfer.Sender?.Address)}`,
        `Receiver: ${toBase58(message.Transfer.Receiver?.Address)}`,
        `Authority: ${toBase58(message.Transfer.Authority?.Address)}`,
        `Currency: ${message.Transfer.Currency?.Symbol || "N/A"}`,
      ];

      bufferedLog(logLines.join("\n"));
    }

    if (message.BalanceUpdate) {
      balanceUpdateCount++;
      const logLines = [
        "\n=== Balance Update ===",
        `Block Slot: ${message.Block?.Slot}`,
        `Timestamp: ${new Date(receivedTimestamp).toISOString()}`,
        `Pre Balance: ${message.BalanceUpdate.BalanceUpdate?.PreBalance}`,
        `Post Balance: ${message.BalanceUpdate.BalanceUpdate?.PostBalance}`,
        `Account Index: ${message.BalanceUpdate.BalanceUpdate?.AccountIndex}`,
        `Currency: ${message.BalanceUpdate.Currency?.Symbol || "N/A"}`,
      ];

      bufferedLog(logLines.join("\n"));
    }

    if (message.Transaction) {
      transactionCount++;
      const logLines = [
        "\n=== Parsed Transaction ===",
        `Block Slot: ${message.Block?.Slot}`,
        `Timestamp: ${new Date(receivedTimestamp).toISOString()}`,
        `Index: ${message.Transaction.Index}`,
        `Signature: ${toBase58(message.Transaction.Signature)}`,
        `Success: ${message.Transaction.Status?.Success}`,
        `Error: ${message.Transaction.Status?.ErrorMessage || "None"}`,
      ];

      const instructions = message.Transaction.ParsedIdlInstructions || [];
      logLines.push(`ParsedIdlInstructions count: ${instructions.length}`);

      const instructionDetails = instructions.map((ix) => {
        const programAddr = ix.Program
          ? toBase58(ix.Program.Address)
          : "unknown";
        const programName = ix.Program?.Name || "";
        const method = ix.Program?.Method || "";
        const accountsCount = (ix.Accounts || []).length;
        return `  #${ix.Index} program=${programAddr} name=${programName} method=${method} accounts=${accountsCount}`;
      });

      logLines.push(...instructionDetails);
      bufferedLog(logLines.join("\n"));
    }
  });

  stream.on("error", (error) => {
    flushLogs();
    console.error("Stream error:", error);
    console.error("Error details:", error.details);
    console.error("Error code:", error.code);
    console.error("Request sent:", JSON.stringify(request, null, 2));
  });

  stream.on("end", () => {
    flushLogs();
    console.log("Stream ended");
  });

  stream.on("status", (status) => {
    bufferedLog(`Stream status: ${JSON.stringify(status)}`);
  });
}

// Handle process termination
process.on("SIGINT", () => {
  flushLogs();
  if (logFlushInterval) clearInterval(logFlushInterval);
  if (statsInterval) clearInterval(statsInterval);
  console.log("\n\nShutting down gracefully...");
  console.log("Final stats:");
  console.log(`Total messages received: ${messageCount}`);
  console.log(`Total trades shown: ${tradeCount}`);
  console.log(`Total filtered out: ${filteredOutCount}`);
  process.exit(0);
});

process.on("SIGTERM", () => {
  flushLogs();
  if (logFlushInterval) clearInterval(logFlushInterval);
  if (statsInterval) clearInterval(statsInterval);
  console.log("\nShutting down gracefully...");
  process.exit(0);
});

// Start listening
try {
  listenToStream();
} catch (error) {
  console.error("Failed to start stream:", error);
  process.exit(1);
}
