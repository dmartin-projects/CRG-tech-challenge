from aiohttp import web
from app.routes import setup_routes

async def init_app():
    app = web.Application()
    setup_routes(app)
    return app

if __name__ == '__main__':
    web.run_app(init_app(), port=8080)
