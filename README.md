# Transaction Processing System

## Overview

This project implements a simple transaction processing system in Rust. It supports the following types of transactions:

- Deposit
- Withdrawal
- Dispute
- Resolve
- Chargeback

The system ensures accurate handling of transactions and maintains the correct state of client accounts.

## Setup

#### Prerequisites

- Rust (latest stable version)

#### Building the project

```
cargo build
```

#### Running the Tests

The project includes unit tests to verify the functionality of the transaction processing system. To run the tests, use:

```
cargo test
```

### Usage

```
cargo run -- transactions.csv > accounts.csv
```
