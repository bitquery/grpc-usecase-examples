# Pump.fun gRPC Streams

Real-time streaming of Pump.fun DEX trades, orders, and transactions via CoreCast gRPC API.

## Repository

🔗 [**GitHub Repository**](https://github.com/Akshat-cs/pump.fun-gRPC-Streams.git)

Clone and get started:

```bash
git clone https://github.com/Akshat-cs/pump.fun-gRPC-Streams.git
```

## Introduction

This Node.js client allows you to stream real-time trading data from Pump.fun (Solana's popular memecoin launchpad) using the CoreCast gRPC API. Monitor track buying/selling pressure, detect whale activity, and analyze trading patterns in real-time.

**Key Features:**

- 🚀 Real-time trade streaming from Pump.fun
- 🎯 Flexible filtering by tokens, traders, and trade direction
- 💰 Separate buy and sell trade monitoring
- 📊 Performance metrics and statistics
- 🔍 Detailed trade information including accounts and currencies
- ⚡ High-performance with caching and buffering

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Configure your filters in config.yaml
# Edit the file to set your desired token and trade filter

# 3. Run the client
node index.js
```

## Stream Types

The client supports multiple stream types:

| Stream Type    | Description                             |
| -------------- | --------------------------------------- |
| `dex_trades`   | Real-time trade events (default)        |
| `dex_orders`   | Order placement and cancellation events |
| `dex_pools`    | Pool liquidity change events            |
| `transactions` | General transaction stream              |
| `transfers`    | Token transfer events                   |
| `balances`     | Balance update events                   |

For Pump.fun monitoring, use **`dex_trades`** (default).

## Trade Data Structure

When you stream Pump.fun trades, each message contains:

### Trade Event Structure

```javascript
{
  Block: {
    Slot: 370485092  // Solana block slot number
  },
  Transaction: {
    Index: 1,
    Signature: "5277PwHQ4PkKRExT45HV8X8XXDmQjWZzHK8dx5ru1eaA...",
    Status: {
      Success: true,
      ErrorMessage: null
    },
    Header: {
      Fee: 5000,
      FeePayer: "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
      Signer: "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
      Accounts: [...]
    }
  },
  Trade: {
    InstructionIndex: 2,
    Dex: {
      ProgramAddress: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
      ProtocolName: "pump_fun",
      ProtocolFamily: "pump_fun"
    },
    Market: {
      MarketAddress: "YcQB1hGSR9hNbJ52zrCJyMvbRViQKiaLfenrgZR9BXY",
      BaseCurrency: {
        Symbol: "PUMPTOKEN",
        Name: "Pump Token",
        MintAddress: "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump",
        Decimals: 6
      },
      QuoteCurrency: {
        Symbol: "SOL",
        Name: "Wrapped SOL",
        MintAddress: "So11111111111111111111111111111111111111112",
        Decimals: 9
      }
    },
    Buy: {
      Amount: 100000000,  // Amount of token being bought
      Currency: {
        Symbol: "PUMPTOKEN",
        Name: "Pump Token",
        MintAddress: "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump",
        Decimals: 6,
        Parsed: true
      },
      Account: {
        Address: "8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m",
        IsSigner: true,
        IsWritable: true
      },
      Order: {
        OrderId: null
      }
    },
    Sell: {
      Amount: 500000000,  // Amount of SOL being sold
      Currency: {
        Symbol: "SOL",
        Name: "Wrapped SOL",
        MintAddress: "So11111111111111111111111111111111111111112",
        Decimals: 9
      },
      Account: {
        Address: "8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m",
        IsSigner: true,
        IsWritable: true
      },
      Order: {
        OrderId: null
      }
    },
    Fee: 0,
    Royalty: 0,
    Instruction: {
      Index: 2,
      Depth: 0,
      Program: {
        Address: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        Name: "pump_fun",
        Method: "swap",
        Parsed: true
      },
      Accounts: [...],
      Logs: [...]
    }
  }
}
```

### Key Fields Explained

| Field                            | Description                                                       |
| -------------------------------- | ----------------------------------------------------------------- |
| `Trade.Buy.Amount`               | Amount of token being bought (in base units)                      |
| `Trade.Sell.Amount`              | Amount of SOL being sold (in lamports)                            |
| `Trade.Buy.Currency.MintAddress` | Token mint address                                                |
| `Trade.Buy.Account.Address`      | Buyer's wallet address                                            |
| `Trade.Dex.ProgramAddress`       | Always `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` for Pump.fun |
| `Trade.Market.MarketAddress`     | Unique market/pool address for the token                          |
| `Block.Slot`                     | Solana block slot for timing analysis                             |

## Configuration Options

Edit `config.yaml` to configure your stream:

### Trade Filter Options

```yaml
trade_filter: "alltrades" # or "buys" or "sells"
```

| Value       | Description                                      |
| ----------- | ------------------------------------------------ |
| `alltrades` | Show all trades (both buys and sells)            |
| `buys`      | Show only trades where the token is being bought |
| `sells`     | Show only trades where the token is being sold   |

### Available Filters

Server-side filters (applied by CoreCast API):

| Filter     | Description                            | Example                                                 |
| ---------- | -------------------------------------- | ------------------------------------------------------- |
| `programs` | Filter by DEX program address          | Pump.fun: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| `tokens`   | Filter by token mint address(es)       | Your token mint address                                 |
| `pool`     | Filter by specific market/pool address | Specific Pump.fun market                                |
| `traders`  | Filter by wallet address(es)           | Specific trader wallets                                 |

Client-side filter (applied by this client):

| Filter         | Description                                      |
| -------------- | ------------------------------------------------ |
| `trade_filter` | Filter by trade direction (buys/sells/alltrades) |

## Filter Examples

### 1. Monitor ALL Trades for a Specific Token

Track all trading activity (both buys and sells) for a specific token on Pump.fun.

```yaml
trade_filter: "alltrades"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  tokens:
    - "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump"
```

**Use Case**: General market monitoring, volume analysis

---

### 2. Monitor Only BUYS for a Specific Token

Track buying pressure - see when traders are accumulating the token.

```yaml
trade_filter: "buys"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  tokens:
    - "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump"
```

**Use Case**: Track accumulation patterns, detect buying momentum

---

### 3. Monitor Only SELLS for a Specific Token

Track selling pressure - detect when traders are dumping the token.

```yaml
trade_filter: "sells"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  tokens:
    - "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump"
```

**Use Case**: Detect sell pressure, identify dumps, risk monitoring

---

### 4. Monitor Multiple Tokens

Track trading activity across multiple Pump.fun tokens simultaneously.

```yaml
trade_filter: "alltrades"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  tokens:
    - "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump"
    - "H15fwzsYWQiGTQBn23sC2QuByu9zSvaXhaDwwVmkX5m9"
    - "AnotherTokenMintAddressHere"
```

**Use Case**: Portfolio tracking, multi-token analysis

---

### 5. Monitor ALL Pump.fun Activity

Stream all trades on Pump.fun (no token filter) - useful for market-wide analysis.

```yaml
trade_filter: "alltrades"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  # No tokens filter = all tokens
```

**Use Case**: Market-wide analytics, new token discovery, volume tracking

**⚠️ Warning**: This will stream a high volume of messages. Ensure your system can handle the load.

---

### 6. Monitor Specific Trader Activity

Track all trades made by a specific wallet on Pump.fun.

```yaml
trade_filter: "alltrades"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  traders:
    - "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
```

**Use Case**: Whale watching, copytrading, smart money tracking

---

### 7. Monitor Specific Trader's Token Buys

Track when a specific wallet buys a specific token.

```yaml
trade_filter: "buys"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  tokens:
    - "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump"
  traders:
    - "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
```

**Use Case**: Copy-trading specific whales, alpha signal detection

---

### 8. Monitor Multiple Traders

Track activity from multiple wallets (whale watching).

```yaml
trade_filter: "alltrades"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  traders:
    - "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
    - "8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m"
    - "9iJkLmNoPQrsTUVxYZaBcDeFgH1JkLmNoPQrsTUVxYZ"
```

**Use Case**: Monitor multiple known profitable traders

---

### 9. Monitor Specific Market/Pool

Track all activity in a specific Pump.fun market.

```yaml
trade_filter: "alltrades"
filters:
  pool:
    - "YcQB1hGSR9hNbJ52zrCJyMvbRViQKiaLfenrgZR9BXY"
```

**Use Case**: Deep dive into a specific token's liquidity pool

---

### 10. Detect Large Buys (Whale Accumulation)

Monitor buy trades and filter large amounts in your application logic.

```yaml
trade_filter: "buys"
filters:
  programs:
    - "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
  tokens:
    - "7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump"
```

Then in your code, filter by `Trade.Buy.Amount` or `Trade.Sell.Amount` (SOL value).

**Use Case**: Whale buy alerts, large transaction monitoring

## Output Example

When a trade matches your filters, you'll see:

```
================================================================================
🟢 BUY Trade
================================================================================
Block Slot: 370485092
Timestamp: 2025-10-01T13:11:32.922Z
Instruction Index: 2

📍 DEX Info:
  Program: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
  Protocol: pump_fun (pump_fun)

🏪 Market Info:
  Address: YcQB1hGSR9hNbJ52zrCJyMvbRViQKiaLfenrgZR9BXY
  Base Currency: PUMPTOKEN
  Quote Currency: SOL

💰 Buy Side:
  Amount: 100000000
  Currency: PUMPTOKEN (Pump Token)
  Mint: 7CyJ5J3tqRKKJjSWSzASuVJdD2oryAJZfyvbMVJmpump
  Decimals: 6
  Account: 8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m
  Is Signer: true
  Is Writable: true
  Order ID: undefined

💸 Sell Side:
  Amount: 500000000
  Currency: SOL (Wrapped SOL)
  Mint: So11111111111111111111111111111111111111112
  Decimals: 9
  Account: 8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m
  Is Signer: true
  Is Writable: true
  Order ID: undefined

💵 Fee: 0
👑 Royalty: 0
================================================================================
```
