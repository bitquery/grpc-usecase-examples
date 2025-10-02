# LetsBonk.fun gRPC Streams

Real-time streaming of LetsBonk.fun DEX trades, orders, and transactions via CoreCast gRPC API.

## Repository

🔗 [**GitHub Repository**](https://github.com/Akshat-cs/letsbonk.fun-gRPC-Streams.git)

Clone and get started:

```bash
git clone https://github.com/Akshat-cs/letsbonk.fun-gRPC-Streams.git
```

## Introduction

This Node.js client allows you to stream real-time trading data from LetsBonk.fun (Solana's memecoin launchpad built on Raydium LaunchLab) using the CoreCast gRPC API. Monitor token launches, track buying/selling pressure, detect whale activity, and analyze trading patterns in real-time.

**Key Features:**

- 🚀 Real-time trade streaming from LetsBonk.fun
- 🎯 Flexible filtering by tokens, traders, and trade direction
- 💰 Separate buy and sell trade monitoring
- 📊 Performance metrics and statistics
- 🔍 Detailed trade information including accounts and currencies
- ⚡ High-performance with caching and buffering

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Get your API token
# Visit: https://account.bitquery.io/user/api_v2/access_tokens

# 3. Configure your filters in config.yaml
# Edit the file to set your auth token and desired filters

# 4. Run the client
node index.js
```

## What is LetsBonk.fun?

LetsBonk.fun tokens are created and traded on **Raydium LaunchLab** (Program: `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj`).

## Trade Data Structure

When you stream LetsBonk.fun trades, each message contains:

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
    }
  },
  Trade: {
    InstructionIndex: 2,
    Dex: {
      ProgramAddress: "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
      ProtocolName: "raydium_launchpad",
      ProtocolFamily: "raydium"
    },
    Market: {
      MarketAddress: "YcQB1hGSR9hNbJ52zrCJyMvbRViQKiaLfenrgZR9BXY",
      BaseCurrency: {
        Symbol: "BONKTOKEN",
        Name: "Bonk Token",
        MintAddress: "CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk",
        Decimals: 6
      },
      QuoteCurrency: {
        Symbol: "SOL",
        MintAddress: "So11111111111111111111111111111111111111112",
        Decimals: 9
      }
    },
    Buy: {
      Amount: 100000000,
      Currency: {
        Symbol: "BONKTOKEN",
        MintAddress: "CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk",
        Decimals: 6
      },
      Account: {
        Address: "8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m",
        IsSigner: true,
        IsWritable: true
      }
    },
    Sell: {
      Amount: 500000000,
      Currency: {
        Symbol: "SOL",
        MintAddress: "So11111111111111111111111111111111111111112",
        Decimals: 9
      },
      Account: {
        Address: "8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m",
        IsSigner: true,
        IsWritable: true
      }
    },
    Fee: 0,
    Royalty: 0
  }
}
```

### Key Fields

| Field                            | Description                       |
| -------------------------------- | --------------------------------- |
| `Trade.Buy.Amount`               | Amount of token being bought      |
| `Trade.Sell.Amount`              | Amount of token being sold        |
| `Trade.Buy.Currency.MintAddress` | Token mint address                |
| `Trade.Buy.Account.Address`      | Buyer's wallet address            |
| `Trade.Dex.ProgramAddress`       | Raydium LaunchLab program address |
| `Trade.Market.MarketAddress`     | Market/pool address for the token |
| `Block.Slot`                     | Solana block slot for timing      |

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

| Filter     | Description                      | Example                                       |
| ---------- | -------------------------------- | --------------------------------------------- |
| `programs` | Filter by DEX program address    | `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj` |
| `tokens`   | Filter by token mint address(es) | Your token mint address                       |
| `traders`  | Filter by wallet address(es)     | Specific trader wallets                       |

## Filter Examples

### 1. Monitor ALL trades for a specific token on LetsBonk.fun

Track all trading activity (both buys and sells) for a specific token.

```yaml
trade_filter: "alltrades"
filters:
  programs:
    - "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
  tokens:
    - "CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk"
```

**Use Case**: General market monitoring, volume analysis

---

### 2. Monitor only BUYS for a specific token on LetsBonk.fun

Track buying pressure - see when traders are accumulating the token.

```yaml
trade_filter: "buys"
filters:
  programs:
    - "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
  tokens:
    - "CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk"
```

**Use Case**: Track accumulation patterns, detect buying momentum

---

### 3. Monitor only SELLS for a specific token on LetsBonk.fun

Track selling pressure - detect when traders are dumping the token.

```yaml
trade_filter: "sells"
filters:
  programs:
    - "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
  tokens:
    - "CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk"
```

**Use Case**: Detect sell pressure, identify dumps, risk monitoring

---

### 4. Monitor multiple tokens on LetsBonk.fun

Track trading activity across multiple tokens simultaneously.

```yaml
trade_filter: "alltrades"
filters:
  programs:
    - "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
  tokens:
    - "CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk"
    - "4FBx5RBfEuuhkT5RB7kJ46WC6cL9J4SJNXyKeoDAbonk"
```

**Use Case**: Portfolio tracking, multi-token analysis

---

### 5. Monitor buying activity for a specific trader

Track when a specific wallet buys a specific token.

```yaml
trade_filter: "buys"
filters:
  programs:
    - "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
  tokens:
    - "CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk"
  traders:
    - "YourWalletAddressHere"
```

**Use Case**: Copy-trading specific whales, alpha signal detection

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
  Program: LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj
  Protocol: raydium_launchpad (raydium)

🏪 Market Info:
  Address: YcQB1hGSR9hNbJ52zrCJyMvbRViQKiaLfenrgZR9BXY
  Base Currency: BONKTOKEN
  Quote Currency: SOL

💰 Buy Side:
  Amount: 100000000
  Currency: BONKTOKEN (Bonk Token)
  Mint: CHNxstQ6zsj9b7QMmCKbnJkhyKwoTL19bPe6VYoebonk
  Decimals: 6
  Account: 8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m
  Is Signer: true
  Is Writable: true

💸 Sell Side:
  Amount: 500000000
  Currency: SOL (Wrapped SOL)
  Mint: So11111111111111111111111111111111111111112
  Decimals: 9
  Account: 8HqR8D9gHtN1eMJyaX7BN5PmzF5z9KgQzY4nXvFfRD8m
  Is Signer: true
  Is Writable: true

💵 Fee: 0
👑 Royalty: 0
================================================================================
```
