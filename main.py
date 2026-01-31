import uuid
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv # NEW
import os
import urllib.parse
# Load variables from .env file
load_dotenv()


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION ---
def get_env_var(name):
    value = os.getenv(name)
    if value is None:
        print(f"❌ ERROR: {name} is not set in the .env file!")
        return "" # Return empty string to avoid NoneType errors
    return value.strip()

KIBANA_URL = get_env_var("KIBANA_URL").rstrip('/')
API_KEY = get_env_var("API_KEY")
MASTER_DB_URL = get_env_var("MASTER_DB_URL")
class ManagerConfig(BaseModel):
    
    full_name: str
    company_name: str
    db_host: str
    db_user: str
    db_pass: str
    db_name: str
    db_port: int

class EcomOpsRequest(BaseModel):
    prompt: str
    conversation_id: str
    manager_id: str  # Added this to link to history
    manager_name: str
    db_config: Optional[Dict] = None 
    agent_id: str = "ecomops_agent12345"



def get_master_db_conn():
    """Helper to create a connection to the primary application database."""
    try:
        conn = psycopg2.connect(host="aws-1-eu-west-1.pooler.supabase.com", 
            database="postgres",
            user="postgres.ghkglgpzemroczxddbul",
            password="@A1g2e3n4t5", 
            port=6543,
            sslmode='require',
            options="-c pgbouncer=true")
        return conn
    except Exception as e:
        print(f"❌ Could not connect to Master DB: {e}")
        raise e
@app.post("/api/v1/register-manager")
async def register_manager(config: ManagerConfig):
    conn = None
    try:
        conn = get_master_db_conn()
        cursor = conn.cursor()
        
        
        query = """
        INSERT INTO manager_configs (full_name, company_name, db_host, db_user, db_pass, db_name, db_port)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING manager_id;
        """
        
        cursor.execute(query, (
            config.full_name, 
            config.company_name, 
            config.db_host, 
            config.db_user, 
            config.db_pass, 
            config.db_name, 
            config.db_port
        ))
        
        # Capture the ID that Supabase just created
        new_id_row = cursor.fetchone()
        new_id = new_id_row[0] if new_id_row else None
        
        conn.commit()
        
        return {
            "status": "success", 
            "manager_id": str(new_id),
            "message": f"Manager {config.full_name} registered!"
        }
    except Exception as e:
        print(f"ERROR: {str(e)}") # This helps you see the error in the terminal
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()
@app.post("/api/v1/converse")
async def converse(request: EcomOpsRequest):
    conn = None
    try:
        conn = get_master_db_conn()
        cursor = conn.cursor()

        # 1. Thread and Message persistence
        cursor.execute("""
            INSERT INTO chat_threads (id, manager_id, title)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (request.conversation_id, request.manager_id, request.prompt[:30] + "..."))

        cursor.execute("""
            INSERT INTO chat_messages (conversation_id, role, content)
            VALUES (%s, %s, %s)
        """, (request.conversation_id, 'user', request.prompt))
        conn.commit()

        # 2. Kibana Request Construction
        # Ensure URL has no trailing slash and API KEY is clean
        
        base_url = KIBANA_URL
        api_endpoint = f"{base_url}/s/default/api/agent_builder/converse"
        
       
        headers = {
            "Authorization": f"ApiKey {API_KEY.strip()}",
            "Content-Type": "application/json",
            "kbn-xsrf": "true"
        }
        
        # Build parameters carefully
        db = request.db_config or {}
        payload = {
            "input": request.prompt,
            "agent_id": request.agent_id,
            "conversation_id": request.conversation_id,
            "parameters": {
                "manager_context": request.manager_name,
                "db_host": db.get("db_host") or db.get("host", ""),
                "db_user": db.get("db_user") or db.get("user", ""),
                "db_pass": db.get("db_pass") or db.get("pass", ""),
                "db_name": db.get("db_name") or db.get("name", ""),
                "db_port": str(db.get("db_port") or db.get("port", "5432"))
            }
        }

        # 3. Call Kibana with detailed error trapping
        response = requests.post(api_endpoint, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 401:
            print(f"❌ 401 UNAUTHORIZED: Check if your API Key has 'All' privileges for 'agentBuilder' in Kibana.")
            raise HTTPException(status_code=401, detail="FastAPI -> Kibana Authentication Failed.")
            
        response.raise_for_status()
        ai_data = response.json()
        ai_message = ai_data.get("response", {}).get("message", "I'm sorry, I couldn't process that.")

        # 4. Save AI Response
        cursor.execute("""
            INSERT INTO chat_messages (conversation_id, role, content)
            VALUES (%s, %s, %s)
        """, (request.conversation_id, 'assistant', ai_message))
        conn.commit()

        return ai_data

    except requests.exceptions.HTTPError as e:
        print(f"🔥 Kibana API Error ({response.status_code}): {response.text}")
        raise HTTPException(status_code=response.status_code, detail=f"Kibana Error: {response.text}")
    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ General Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()
        
@app.get("/api/v1/history/{conversation_id}")
async def get_chat_history(conversation_id: str):
    conn = None
    try:
        conn = get_master_db_conn()
        # RealDictCursor makes the rows look like JSON objects automatically
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT role, content, created_at 
            FROM chat_messages 
            WHERE conversation_id = %s 
            ORDER BY created_at ASC
        """, (conversation_id,))
        
        history = cursor.fetchall()
        return {"history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()
        
@app.get("/api/v1/threads/{manager_id}")
async def get_manager_threads(manager_id: str):
    
    try:
        uuid.UUID(str(manager_id))
    except ValueError:
        # If it's not a valid UUID (like "default_manager"), return empty list instead of crashing
        return {"threads": []}
    conn = None
    try:
        conn = get_master_db_conn()
        # Using RealDictCursor so the frontend gets a clean JSON object
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # We fetch the ID and the Title, ordered by the most recent chat first
        cursor.execute("""
            SELECT id, title, created_at 
            FROM chat_threads 
            WHERE manager_id = %s 
            ORDER BY created_at DESC
        """, (manager_id,))
        
        threads = cursor.fetchall()
        return {"threads": threads}
        
    except Exception as e:
        print(f"Error fetching threads: {e}")
        raise HTTPException(status_code=500, detail="Could not load chat history list.")
    finally:
        if conn:
            conn.close()
            
            
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)