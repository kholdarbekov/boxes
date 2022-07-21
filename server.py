import asyncio
import logging
import pytz
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
from pymongo.errors import DuplicateKeyError

log = logging.getLogger(__name__)
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
        status = RequestStatus.OK
        try:
            _ = boxes_db.boxes.insert_one(data)
        except DuplicateKeyError as exc:
            log.error(f"DuplicateKeyError exception: data={str(data)}, errmsg={str(exc.details)}")
            status = RequestStatus.ERROR
        return CreateBoxResponse(status=status)

    async def update_box(self, box: "Box") -> "UpdateBoxResponse":
        new_box_dict = asdict(box, dict_factory=box_to_dict)
        _update_result = boxes_db.boxes.update_one(
            {
                "_id": box.id
            }, 
            {
                "$set": new_box_dict
            }
        )
        if _update_result.modified_count:
            status = RequestStatus.OK
        else:
            status = RequestStatus.ERROR
        return UpdateBoxResponse(status=status)

    async def delete_box(self, id: int) -> "DeleteBoxResponse":
        _delete_result = boxes_db.boxes.delete_one({"_id": id})
        if _delete_result.deleted_count:
            status = RequestStatus.OK
        else:
            status = RequestStatus.ERROR
        return DeleteBoxResponse(status=status)

    async def get_boxes_in_category(self, category: str) -> "GetBoxesResponse":
        boxes = boxes_db.boxes.find({"category": category})
        list_of_boxes = [dict_to_box(box) for box in boxes]
        return GetBoxesResponse(box=list_of_boxes, status=RequestStatus.OK)

    async def get_boxes_in_time_range(
        self, start_time: datetime, end_time: datetime
    ) -> "GetBoxesResponse":
        boxes = boxes_db.boxes.find(
            {"created_at": {"$gte": start_time, "$lte": end_time}}
        )
        list_of_boxes = [dict_to_box(box) for box in boxes]
        return GetBoxesResponse(box=list_of_boxes, status=RequestStatus.OK)


async def main():
    server = Server([DatabaseService()])
    await server.start("127.0.0.1", 50051)
    await server.wait_closed()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
