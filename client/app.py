import os
import asyncio

from flask import Flask, render_template
from grpclib.client import Channel

import db

app = Flask(__name__)

SERVICE_HOST = os.getenv("SERVICE_HOST", "127.0.0.1")
SERVICE_PORT = os.getenv("SERVICE_PORT", 50051)


@app.route("/")
async def render_homepage():
    service_channel = Channel(f"{SERVICE_HOST}:{SERVICE_PORT}")
    service = db.DatabaseServiceStub(service_channel)

    get_boxes_response = await service.get_boxes()

    service_channel.close()
    return await render_template(
        "index.html",
        boxes=get_boxes_response.box,
    )

if __name__ == '__main__':
    app.run()
