import asyncio
from datetime import datetime
from db import (
    DatabaseServiceBase,
    GetBoxResponse,
    Box,
    RequestStatus,
    GetBoxesResponse,
    CreateBoxResponse,
    UpdateBoxResponse,
    DeleteBoxResponse,
)
from grpclib.server import Server
from typing import AsyncIterator

from db_manager import get_database
from dataclasses import asdict

boxes_db = get_database()


def box_to_dict(box_fields):
    # Converts id field to _id
    # This is sensetive to the position of the field
    box_fields[1] = ("_id", box_fields[1][1])

    return dict(box_fields)


def dict_to_box(data):
    if "_id" in data:
        data["id"] = data.pop("_id")
    return Box(**data)


class DatabaseService(DatabaseServiceBase):
    async def get_box(self, id: int) -> "GetBoxResponse":
        data = boxes_db.boxes.find_one({"_id": id})
        status = RequestStatus.ERROR
        if data:
            data = dict_to_box(data)
            status = RequestStatus.OK
        return GetBoxResponse(box=data, status=status)

    async def get_boxes(self) -> "GetBoxesResponse":
        boxes = boxes_db.boxes.find()
        list_of_boxes = [dict_to_box(box) for box in boxes]
        return GetBoxesResponse(box=list_of_boxes, status=RequestStatus.OK)

    async def create_box(self, box: "Box") -> "CreateBoxResponse":
        if not box.created_at:
            box.created_at = datetime.utcnow()
        data = asdict(box, dict_factory=box_to_dict)
        _ = boxes_db.boxes.insert_one(data)
        return CreateBoxResponse(status=RequestStatus.OK)

    async def update_box(self, box: "Box") -> "UpdateBoxResponse":
        _ = boxes_db.boxes.update_one(asdict(box, dict_factory=box_to_dict))
        return UpdateBoxResponse(status=RequestStatus.OK)

    async def delete_box(self, id: int) -> "DeleteBoxResponse":
        _ = boxes_db.boxes.delete_one({"_id": id})
        return DeleteBoxResponse(status=RequestStatus.OK)

    async def get_boxes_in_category(self, category: str) -> "GetBoxesResponse":
        boxes = boxes_db.boxes.find({"category": category})
        list_of_boxes = [Box(**box) for box in boxes]
        return GetBoxesResponse(box=list_of_boxes, status=RequestStatus.OK)

    async def get_boxes_in_time_range(
        self, start_time: datetime, end_time: datetime
    ) -> "GetBoxesResponse":
        boxes = boxes_db.boxes.find(
            {"created_at": {"$gte": start_time, "$lte": end_time}}
        )
        list_of_boxes = [Box(**box) for box in boxes]
        return GetBoxesResponse(box=list_of_boxes, status=RequestStatus.OK)


async def main():
    server = Server([DatabaseService()])
    await server.start("127.0.0.1", 50051)
    await server.wait_closed()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
