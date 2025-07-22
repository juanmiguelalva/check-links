import asyncio
import aiohttp
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI()

CONCURRENT_REQUESTS = 100
TIMEOUT = aiohttp.ClientTimeout(total=20)
API_TOKENS = {"marketing-cloud-token": "mc-user"}

security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if token not in API_TOKENS:
        raise HTTPException(status_code=403, detail="Invalid or missing token")
    return API_TOKENS[token]

class LinkItem(BaseModel):
    Sku: Optional[str]
    Pais: Optional[str]
    LinkUrl: str

async def check_link(sem, session, item: LinkItem):
    url = item.LinkUrl
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url

    async with sem:
        try:
            async with session.head(url, allow_redirects=True, timeout=TIMEOUT) as response:
                if response.status >= 400:
                    return {
                        "Sku": item.Sku,
                        "Pais": item.Pais,
                        "LinkUrl": item.LinkUrl,
                        "Status": "Link Error"
                    }
        except:
            try:
                async with session.get(url, allow_redirects=True, timeout=TIMEOUT) as response:
                    if response.status >= 400:
                        return {
                            "Sku": item.Sku,
                            "Pais": item.Pais,
                            "LinkUrl": item.LinkUrl,
                            "Status": "Link Error"
                        }
                    else:
                        return None
            except:
                return {
                    "Sku": item.Sku,
                    "Pais": item.Pais,
                    "LinkUrl": item.LinkUrl,
                    "Status": "Browser exception Error"
                }
    return None

async def get_broken_links_async(items: List[LinkItem]):
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=connector) as session:
        tasks = [check_link(sem, session, item) for item in items]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

@app.post("/check-links")
async def check_links(items: List[LinkItem], user: str = Depends(verify_token)):
    if not items:
        raise HTTPException(status_code=400, detail="Request must be a non-empty JSON array")

    try:
        broken_links = await get_broken_links_async(items)
        return {
            "count": len(broken_links),
            "broken_links": broken_links
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

