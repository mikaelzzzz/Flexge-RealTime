# Flexge → Notion Sync Service

Serviço FastAPI que sincroniza dados de estudo da Flexge para o Notion, com limpeza semanal automática.

## Funcionalidades

* **Sincronização a cada 10 minutos**: Busca alunos na Flexge e atualiza/cria páginas no Notion com:
  * Nome do aluno
  * Nível atual
  * Horas de estudo na semana

* **Limpeza Semanal**: Toda segunda-feira às 02:00 UTC:
  * Arquiva todas as páginas do database
  * Limpa o cache de duplicados
  * Prepara o database para uma nova semana

* **Proteção contra Duplicados**: 
  * Cache em memória de páginas existentes
  * Verificação em tempo real antes de cada inserção

## Configuração

1. Crie um arquivo `.env` com as seguintes variáveis:

```bash
NOTION_API_KEY=seu_token_notion
NOTION_DB_ID=id_do_database_notion
FLEXGE_API_KEY=sua_chave_api_flexge
FLEXGE_API_BASE=https://partner-api.flexge.com/external
```

2. Instale as dependências:

```bash
pip install -r requirements.txt
```

3. Execute o serviço:

```bash
uvicorn main:app --reload
```

## Endpoints

* `GET /health`: Verifica status do serviço
* `POST /sync`: Dispara sincronização manual

## Estrutura do Database Notion

O database deve ter as seguintes propriedades:

* `Nome`: Title
* `Nível`: Multi-select
* `Horas de Estudo`: Rich text

## Desenvolvimento

* Python 3.9+
* FastAPI para API REST
* APScheduler para jobs recorrentes
* httpx para requisições assíncronas
* notion-client para integração com Notion

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