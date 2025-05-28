from aiohttp import web
from app.routes import setup_routes
from scripts.db import init_db,close_db


async def init_app():
    app = web.Application()
    app.on_startup.append(init_db)
    app.on_cleanup.append(close_db)
    setup_routes(app)
    return app

if __name__ == '__main__':
    web.run_app(init_app(), port=8080)
