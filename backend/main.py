from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
import sqlite3, json, os, shutil, uuid, asyncio
from datetime import datetime

app = FastAPI(title="BreiserMarkets API")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_PATH     = "/data/breiser.db"
IMAGES_PATH = "/data/images"
os.makedirs(IMAGES_PATH, exist_ok=True)
app.mount("/images", StaticFiles(directory=IMAGES_PATH), name="images")

# ─── DB INIT ─────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS markets (
            id   TEXT PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS products (
            id         TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            category   TEXT DEFAULT '',
            price      REAL NOT NULL,
            image_url  TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS stock (
            id         TEXT PRIMARY KEY,
            market_id  TEXT NOT NULL,
            product_id TEXT NOT NULL,
            size       TEXT NOT NULL,
            qty        INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(market_id)  REFERENCES markets(id),
            FOREIGN KEY(product_id) REFERENCES products(id),
            UNIQUE(market_id, product_id, size)
        );
        CREATE TABLE IF NOT EXISTS sales (
            id           TEXT PRIMARY KEY,
            market_id    TEXT NOT NULL,
            date         TEXT NOT NULL,
            time         TEXT NOT NULL,
            payment      TEXT NOT NULL DEFAULT 'cash',
            total        REAL NOT NULL DEFAULT 0,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS sale_items (
            id         TEXT PRIMARY KEY,
            sale_id    TEXT NOT NULL,
            product_id TEXT NOT NULL,
            size       TEXT NOT NULL,
            qty        INTEGER NOT NULL,
            price      REAL NOT NULL,
            FOREIGN KEY(sale_id) REFERENCES sales(id)
        );
    """)
    # Seed default markets
    c.execute("SELECT COUNT(*) FROM markets")
    if c.fetchone()[0] == 0:
        for name in ["Market Centro", "Market Norte", "Market Sur"]:
            c.execute("INSERT INTO markets VALUES (?,?)", (str(uuid.uuid4()), name))
    conn.commit()
    conn.close()

init_db()

# ─── WEBSOCKET MANAGER ────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.connections: dict[str, list[WebSocket]] = {}

    async def connect(self, ws: WebSocket, market_id: str):
        await ws.accept()
        self.connections.setdefault(market_id, []).append(ws)

    def disconnect(self, ws: WebSocket, market_id: str):
        if market_id in self.connections:
            self.connections[market_id] = [c for c in self.connections[market_id] if c != ws]

    async def broadcast(self, market_id: str, data: dict):
        dead = []
        for ws in self.connections.get(market_id, []):
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, market_id)

    async def broadcast_all(self, data: dict):
        for mid in list(self.connections.keys()):
            await self.broadcast(mid, data)

manager = ConnectionManager()

@app.websocket("/ws/{market_id}")
async def websocket_endpoint(ws: WebSocket, market_id: str):
    await manager.connect(ws, market_id)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, market_id)

# ─── MARKETS ─────────────────────────────────────────────────────────────────
@app.get("/markets")
def get_markets():
    conn = get_db()
    rows = conn.execute("SELECT * FROM markets ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]

class MarketIn(BaseModel):
    name: str

@app.post("/markets")
async def create_market(m: MarketIn):
    conn = get_db()
    mid = str(uuid.uuid4())
    conn.execute("INSERT INTO markets VALUES (?,?)", (mid, m.name))
    conn.commit()
    conn.close()
    market = {"id": mid, "name": m.name}
    await manager.broadcast_all({"type": "market_created", "market": market})
    return market

@app.delete("/markets/{mid}")
async def delete_market(mid: str):
    conn = get_db()
    conn.execute("DELETE FROM stock WHERE market_id=?", (mid,))
    conn.execute("DELETE FROM markets WHERE id=?", (mid,))
    conn.commit()
    conn.close()
    await manager.broadcast_all({"type": "market_deleted", "market_id": mid})
    return {"ok": True}

# ─── PRODUCTS ────────────────────────────────────────────────────────────────
@app.get("/products")
def get_products():
    conn = get_db()
    rows = conn.execute("SELECT * FROM products ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]

class ProductIn(BaseModel):
    name: str
    category: str = ""
    price: float

@app.post("/products")
async def create_product(p: ProductIn):
    conn = get_db()
    pid = str(uuid.uuid4())
    conn.execute("INSERT INTO products(id,name,category,price) VALUES (?,?,?,?)",
                 (pid, p.name, p.category, p.price))
    conn.commit()
    product = {"id": pid, "name": p.name, "category": p.category, "price": p.price, "image_url": ""}
    conn.close()
    await manager.broadcast_all({"type": "product_created", "product": product})
    return product

@app.put("/products/{pid}")
async def update_product(pid: str, p: ProductIn):
    conn = get_db()
    conn.execute("UPDATE products SET name=?,category=?,price=? WHERE id=?",
                 (p.name, p.category, p.price, pid))
    conn.commit()
    row = conn.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    conn.close()
    product = dict(row)
    await manager.broadcast_all({"type": "product_updated", "product": product})
    return product

@app.delete("/products/{pid}")
async def delete_product(pid: str):
    conn = get_db()
    img = conn.execute("SELECT image_url FROM products WHERE id=?", (pid,)).fetchone()
    if img and img["image_url"]:
        fname = img["image_url"].split("/images/")[-1]
        path  = os.path.join(IMAGES_PATH, fname)
        if os.path.exists(path): os.remove(path)
    conn.execute("DELETE FROM stock WHERE product_id=?",   (pid,))
    conn.execute("DELETE FROM products WHERE id=?",        (pid,))
    conn.commit()
    conn.close()
    await manager.broadcast_all({"type": "product_deleted", "product_id": pid})
    return {"ok": True}

@app.post("/products/{pid}/image")
async def upload_image(pid: str, file: UploadFile = File(...)):
    ext      = os.path.splitext(file.filename)[1].lower()
    fname    = f"{pid}{ext}"
    filepath = os.path.join(IMAGES_PATH, fname)
    with open(filepath, "wb") as f:
        shutil.copyfileobj(file.file, f)
    image_url = f"/images/{fname}"
    conn = get_db()
    conn.execute("UPDATE products SET image_url=? WHERE id=?", (image_url, pid))
    conn.commit()
    conn.close()
    await manager.broadcast_all({"type": "product_image", "product_id": pid, "image_url": image_url})
    return {"image_url": image_url}

# ─── STOCK ───────────────────────────────────────────────────────────────────
@app.get("/stock/{market_id}")
def get_stock(market_id: str):
    conn = get_db()
    rows = conn.execute("""
        SELECT s.*, p.name as product_name, p.category, p.price, p.image_url
        FROM stock s JOIN products p ON s.product_id=p.id
        WHERE s.market_id=? AND s.qty>0
        ORDER BY p.name, s.size
    """, (market_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

class StockIn(BaseModel):
    product_id: str
    size: str
    qty: int

@app.post("/stock/{market_id}")
async def set_stock(market_id: str, s: StockIn):
    conn = get_db()
    sid = str(uuid.uuid4())
    conn.execute("""
        INSERT INTO stock(id,market_id,product_id,size,qty) VALUES(?,?,?,?,?)
        ON CONFLICT(market_id,product_id,size) DO UPDATE SET qty=excluded.qty
    """, (sid, market_id, s.product_id, s.size, s.qty))
    conn.commit()
    conn.close()
    await manager.broadcast(market_id, {"type": "stock_updated", "market_id": market_id,
                                         "product_id": s.product_id, "size": s.size, "qty": s.qty})
    return {"ok": True}

# ─── SALES ───────────────────────────────────────────────────────────────────
class SaleItemIn(BaseModel):
    product_id: str
    size: str
    qty: int
    price: float

class SaleIn(BaseModel):
    market_id: str
    payment: str  # 'cash' | 'card'
    items: List[SaleItemIn]

@app.post("/sales")
async def create_sale(sale: SaleIn):
    conn = get_db()
    now   = datetime.now()
    sid   = str(uuid.uuid4())
    total = sum(i.qty * i.price for i in sale.items)

    # Verify + deduct stock
    for item in sale.items:
        row = conn.execute("""
            SELECT qty FROM stock WHERE market_id=? AND product_id=? AND size=?
        """, (sale.market_id, item.product_id, item.size)).fetchone()
        if not row or row["qty"] < item.qty:
            conn.close()
            raise HTTPException(400, f"Stock insuficiente: {item.product_id} talla {item.size}")

    conn.execute("INSERT INTO sales(id,market_id,date,time,payment,total) VALUES(?,?,?,?,?,?)",
                 (sid, sale.market_id, now.strftime("%Y-%m-%d"), now.strftime("%H:%M"), sale.payment, total))

    for item in sale.items:
        conn.execute("INSERT INTO sale_items(id,sale_id,product_id,size,qty,price) VALUES(?,?,?,?,?,?)",
                     (str(uuid.uuid4()), sid, item.product_id, item.size, item.qty, item.price))
        conn.execute("""
            UPDATE stock SET qty=qty-? WHERE market_id=? AND product_id=? AND size=?
        """, (item.qty, sale.market_id, item.product_id, item.size))

    conn.commit()

    # Build broadcast payload
    stock_rows = conn.execute("""
        SELECT product_id, size, qty FROM stock
        WHERE market_id=? AND product_id IN ({})
    """.format(",".join("?" * len(sale.items))),
        [sale.market_id] + [i.product_id for i in sale.items]).fetchall()
    conn.close()

    await manager.broadcast(sale.market_id, {
        "type": "sale_completed",
        "sale_id": sid,
        "stock_updates": [dict(r) for r in stock_rows]
    })

    return {"id": sid, "total": total}

@app.get("/sales/{market_id}")
def get_sales(market_id: str, date: Optional[str] = None):
    conn = get_db()
    query = "SELECT * FROM sales WHERE market_id=?"
    params = [market_id]
    if date:
        query += " AND date=?"
        params.append(date)
    query += " ORDER BY created_at DESC"
    sales_rows = conn.execute(query, params).fetchall()
    result = []
    for s in sales_rows:
        items = conn.execute("""
            SELECT si.*, p.name as product_name, p.image_url
            FROM sale_items si JOIN products p ON si.product_id=p.id
            WHERE si.sale_id=?
        """, (s["id"],)).fetchall()
        result.append({**dict(s), "items": [dict(i) for i in items]})
    conn.close()
    return result

@app.put("/sales/{sale_id}")
async def update_sale(sale_id: str, data: dict):
    conn = get_db()
    conn.execute("UPDATE sales SET payment=? WHERE id=?", (data.get("payment"), sale_id))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.delete("/sales/{sale_id}")
async def delete_sale(sale_id: str):
    conn = get_db()
    sale = conn.execute("SELECT * FROM sales WHERE id=?", (sale_id,)).fetchone()
    if not sale:
        conn.close()
        raise HTTPException(404, "Venta no encontrada")
    items = conn.execute("SELECT * FROM sale_items WHERE sale_id=?", (sale_id,)).fetchall()
    # Restore stock
    for item in items:
        conn.execute("""
            UPDATE stock SET qty=qty+? WHERE market_id=? AND product_id=? AND size=?
        """, (item["qty"], sale["market_id"], item["product_id"], item["size"]))
    conn.execute("DELETE FROM sale_items WHERE sale_id=?", (sale_id,))
    conn.execute("DELETE FROM sales WHERE id=?",           (sale_id,))
    conn.commit()
    stock_rows = conn.execute("SELECT product_id,size,qty FROM stock WHERE market_id=?",
                               (sale["market_id"],)).fetchall()
    conn.close()
    await manager.broadcast(sale["market_id"], {
        "type": "sale_deleted", "sale_id": sale_id,
        "stock_updates": [dict(r) for r in stock_rows]
    })
    return {"ok": True}

@app.get("/export/{market_id}/csv")
def export_csv(market_id: str, date: Optional[str] = None):
    from fastapi.responses import StreamingResponse
    import io
    conn = get_db()
    query = """
        SELECT s.date, s.time, s.payment, si.qty, si.price, si.qty*si.price as total,
               p.name as product, p.category, si.size
        FROM sales s
        JOIN sale_items si ON s.id=si.sale_id
        JOIN products p    ON si.product_id=p.id
        WHERE s.market_id=?
    """
    params = [market_id]
    if date:
        query += " AND s.date=?"
        params.append(date)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    output = io.StringIO()
    output.write("Fecha,Hora,Pago,Prenda,Categoría,Talla,Cantidad,Precio,Total\n")
    for r in rows:
        output.write(f"{r['date']},{r['time']},{r['payment']},{r['product']},{r['category']},{r['size']},{r['qty']},{r['price']:.2f},{r['total']:.2f}\n")
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=ventas.csv"})
