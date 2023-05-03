import os
import time
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

import controller


async def post_method(request):
    
    t1 = time.perf_counter()
    data: dict = await request.json() 
    print('Request', data)
    path = os.path.dirname(__file__)
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = os.getenv('REDIS_PORT', 6379)

    resp = controller.Controller(path, data, redis_host, redis_port).run() 
    print('Response', resp)
    print(f'=== Execution time: {time.perf_counter() - t1}')

    return JSONResponse(resp)

def startup():
    print('Starlette started')
    

routes = [
    Route('/post', post_method, methods=['POST']),
]

app = Starlette(debug=True, routes=routes, on_startup=[startup])