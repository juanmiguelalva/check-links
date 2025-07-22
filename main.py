import asyncio
import aiohttp
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

app = FastAPI()

CONCURRENT_REQUESTS = 500
TIMEOUT = aiohttp.ClientTimeout(total=20)
API_TOKENS = {"marketing-cloud-token": "mc-user"}

security = HTTPBearer()

HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; LinkChecker/1.0)'}

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if token not in API_TOKENS:
        raise HTTPException(status_code=403, detail="Invalid or missing token")
    return API_TOKENS[token]

class LinkItem(BaseModel):
    Sku: Optional[str]
    Pais: Optional[str]
    LinkUrl: str

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme:
        parsed = parsed._replace(scheme='http')
    if parsed.netloc and not parsed.netloc.startswith('www.'):
        new_netloc = 'www.' + parsed.netloc
        parsed = parsed._replace(netloc=new_netloc)
    return urlunparse(parsed)

async def check_link(sem, session, item: LinkItem):
    url = normalize_url(item.LinkUrl)

    async with sem:
        try:
            async with session.head(url, allow_redirects=True, timeout=TIMEOUT, headers=HEADERS) as response:
                if response.status >= 400:
                    return {
                        "Sku": item.Sku,
                        "Pais": item.Pais,
                        "LinkUrl": item.LinkUrl,
                        "Status": "Link Error"
                    }
        except Exception as e1:
            try:
                async with session.get(url, allow_redirects=True, timeout=TIMEOUT, headers=HEADERS) as response:
                    if response.status >= 400:
                        return {
                            "Sku": item.Sku,
                            "Pais": item.Pais,
                            "LinkUrl": item.LinkUrl,
                            "Status": str(e1)
                        }
                    else:
                        return None
            except Exception as e2:
                return {
                    "Sku": item.Sku,
                    "Pais": item.Pais,
                    "LinkUrl": item.LinkUrl,
                    "Status": str(e2)
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

