# Flexge-Notion Sync Service

A FastAPI application that synchronizes Flexge study-hour data with Notion databases in real-time.

## Features

- Polls Flexge API every minute for fresh study-hour data
- Synchronizes data to specified Notion databases
- Double-duplicate protection mechanism
- OpenAI integration for text enhancement
- Background task scheduling with APScheduler
- RESTful endpoints for health checks and manual sync

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/flexge-notion-sync.git
cd flexge-notion-sync
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file with the following variables:
```
NOTION_API_KEY=your_notion_api_key
FLEXGE_API_KEY=your_flexge_api_key
FLEXGE_API_BASE=https://partner-api.flexge.com/external
OPENAI_API_KEY=your_openai_api_key
```

4. Run the application:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API Endpoints

- `GET /health`: Check service health
- `POST /sync`: Manually trigger Flexge sync

## Deployment

This service is designed to be deployed on Render. Follow these steps:

1. Push your code to GitHub
2. Create a new Web Service on Render
3. Connect to your GitHub repository
4. Set the following:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Add your environment variables in Render's dashboard

## License

MIT 