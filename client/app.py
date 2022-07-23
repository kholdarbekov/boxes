from datetime import datetime
import os

from flask import Flask, redirect, render_template, request, url_for
from marshmallow import Schema, fields
from grpclib.client import Channel

import db
from db import RequestStatus

app = Flask(__name__)

SERVICE_HOST = os.getenv("APP_HOST", "127.0.0.1")
SERVICE_PORT = os.getenv("APP_PORT", 50051)


class GetBoxesSchema(Schema):
    category = fields.Str()
    # ex: 2014-12-22T03:12:58.019077+00:00
    start_time = fields.DateTime()
    end_time = fields.DateTime()


@app.route("/box/<int:id>")
async def get_box(id):
    service_channel = Channel(host=SERVICE_HOST, port=SERVICE_PORT)
    service = db.DatabaseServiceStub(service_channel)

    get_box_response = await service.get_box(id=id)

    service_channel.close()
    if get_box_response.status == RequestStatus.OK:
        return render_template("get_box.html", box=get_box_response.box)
    else:
        return render_template("404.html")


@app.route("/")
async def get_boxes():
    service_channel = Channel(host=SERVICE_HOST, port=SERVICE_PORT)
    service = db.DatabaseServiceStub(service_channel)

    args = GetBoxesSchema().load(request.args)

    if "category" in args:
        get_boxes_response = await service.get_boxes_in_category(
            category=args.get("category")
        )
    elif "start_time" in args and "end_time" in args:
        # ex datetime: 2014-12-22T03:12:58.019077+00:00
        get_boxes_response = await service.get_boxes_in_time_range(
            start_time=args.get("start_time"), end_time=args.get("end_time")
        )
    else:
        get_boxes_response = await service.get_boxes()

    service_channel.close()
    return render_template("get_boxes.html", boxes=get_boxes_response.box)


@app.route("/create_box", methods=("GET", "POST"))
async def create_box():
    err = None
    if request.method == "POST":
        service_channel = Channel(host=SERVICE_HOST, port=SERVICE_PORT)
        service = db.DatabaseServiceStub(service_channel)

        name = request.form["Name"]
        id = request.form["Id"]
        price = request.form["Price"]
        description = request.form["Description"]
        category = request.form["Category"]
        quantity = request.form["Quantity"]

        box = db.Box(
            name=name,
            id=int(id) if id else None,
            price=int(price) if price else None,
            description=description,
            category=category,
            quantity=int(quantity) if quantity else None,
            created_at=datetime.utcnow(),
        )

        create_box_response = await service.create_box(box=box)
        service_channel.close()

        if create_box_response.status == RequestStatus.OK:
            return redirect(url_for("get_boxes"))
        else:
            err = "Error on creating new box"

    return render_template("create_box.html", error=err)


@app.route("/update_box/<int:id>", methods=("GET", "POST"))
async def update_box(id=None):
    service_channel = Channel(host=SERVICE_HOST, port=SERVICE_PORT)
    service = db.DatabaseServiceStub(service_channel)
    err = None
    if request.method == "POST":
        name = request.form["Name"]
        price = request.form["Price"]
        description = request.form["Description"]
        category = request.form["Category"]
        quantity = request.form["Quantity"]

        box = db.Box(
            name=name,
            id=int(id) if id else None,
            price=int(price) if price else None,
            description=description,
            category=category,
            quantity=int(quantity) if quantity else None,
            created_at=datetime.utcnow(),
        )

        update_box_response = await service.update_box(box=box)
        service_channel.close()
        if update_box_response.status == RequestStatus.OK:
            return redirect(url_for("get_box", id=id))
        else:
            err = "Error on updating the box"

    # GET method part
    get_box_response = await service.get_box(id=id)
    service_channel.close()

    if get_box_response.status == RequestStatus.OK:
        return render_template("update_box.html", box=get_box_response.box, error=err)
    else:
        return render_template("404.html")


@app.route("/box/<int:id>", methods=("POST",))
async def delete_box(id):
    service_channel = Channel(host=SERVICE_HOST, port=SERVICE_PORT)
    service = db.DatabaseServiceStub(service_channel)

    delete_box_response = await service.delete_box(id=id)

    service_channel.close()
    return redirect(url_for("get_boxes"))


if __name__ == "__main__":
    app.run()
