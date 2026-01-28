from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional,Dict
import requests
import os

app = FastAPI()

# Enable CORS so your HTML file can talk to FastAPI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows any origin to connect
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# --- CONFIGURATION ---
KIBANA_URL = "https://my-elasticsearch-project-eb5aee.kb.us-central1.gcp.elastic.cloud"
AGENT_ID = "ecomops_agent12345"
API_KEY = "dVIyaTlwc0JHMVBiTUQzR1QtZ3c6TnloOTRiUWZ5Q013UThNLXBQQ2Zodw=="

class EcomOpsRequest(BaseModel):
    prompt: str
    conversation_id: Optional[str] = None
    manager_name: str
    # We add this to receive the DB details from your index.html
    db_config: Optional[Dict] = None 
    agent_id: str = "ecomops_agent12345"

@app.post("/api/v1/converse")
async def converse(request: EcomOpsRequest):
    url = f"{KIBANA_URL}/api/agent_builder/converse"
    headers = {
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type": "application/json",
        "kbn-xsrf": "true"
    }
    
    # 2. Inject the DB config into the parameters
    # This allows the MCP tools to access these credentials
    payload = {
        "input": request.prompt,
        "agent_id": request.agent_id,
        "conversation_id": request.conversation_id,
        "parameters": {
            "manager_context": request.manager_name,
            "db_host": request.db_config.get("host") if request.db_config else None,
            "db_port": request.db_config.get("port") if request.db_config else None,
            "db_user": request.db_config.get("user") if request.db_config else None,
            "db_pass": request.db_config.get("pass") if request.db_config else None,
            "db_name": request.db_config.get("db") if request.db_config else None
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"EcomOps Error: {str(e)}")
        raise HTTPException(status_code=500, detail="EcomOps Agent is currently unavailable.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)