# Lighter Explorer

Web dashboard for the [Lighter](https://lighter.xyz) perpetual futures exchange. Look up any L1 address to view its main account and all sub-accounts with positions, balances, and trading details.

## Features

- Search accounts by Ethereum L1 address
- Main account summary (collateral, balance, active subs count)
- Sub-accounts table with sortable columns
- Expandable rows with detailed account info and open positions
- Filters: with balance, in position, show zero positions
- Search by account index
- Address history (localStorage)

## Quick Start

```bash
cp .env.example .env
docker compose up --build
```

Open [http://localhost:8100](http://localhost:8100).

## Configuration

| Variable | Default | Description |
|---|---|---|
| `LIGHTER_BASE_URL` | `https://mainnet.zklighter.elliot.ai` | Lighter API base URL |

## Project Structure

```
├── backend/
│   ├── app.py              # FastAPI server, proxies Lighter API
│   └── requirements.txt
├── frontend/
│   ├── index.html
│   ├── app.js
│   └── style.css
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── .gitignore
```

## Tech Stack

- **Backend**: Python, FastAPI, httpx
- **Frontend**: Vanilla HTML/CSS/JS
- **Deploy**: Docker

## License

[MIT](LICENSE)
