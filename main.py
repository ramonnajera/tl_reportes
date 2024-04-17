from os import getenv
from app.api import app
import uvicorn
from dotenv import load_dotenv

load_dotenv('.env')

if __name__ == "__main__":
    host = getenv('HOST', '0.0.0.0')
    port = int(getenv('PORT', 8000))
    uvicorn.run(app, host=host, port=port, reload=True)