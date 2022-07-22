import asyncio
import db
from datetime import datetime
from grpclib.client import Channel


async def main():
    channel = Channel(host="127.0.0.1", port=50051)
    service = db.DatabaseServiceStub(channel)
    box = db.Box(
        name="Box 3 new",
        id=3,
        price=102,
        description="some new description",
        category="CATEGORY 1",
        #created_at=datetime.now(),
    )
    response = await service.create_box(box=box)
    #response = await service.update_box(box=box)
    #response = await service.delete_box(id=4)
    #response = await service.get_boxes_in_category(category="CATEGORY 1")
    
    start_time_str  = "21/7/2022 8:18:19"
    end_time_str  = "21/7/2022 12:18:19"
    format  = "%d/%m/%Y %H:%M:%S"
    # Create datetime object in local timezone
    start_time = datetime.strptime(start_time_str, format)
    end_time = datetime.strptime(end_time_str, format)
    
    response = await service.get_boxes_in_time_range(start_time=start_time, end_time=end_time)
    
    response = await service.get_boxes()
    print(response)

    # don't forget to close the channel when done!
    channel.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
