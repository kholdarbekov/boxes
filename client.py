import asyncio
import db

from grpclib.client import Channel


async def main():
    channel = Channel(host="127.0.0.1", port=50051)
    service = db.DatabaseServiceStub(channel)
    box = db.Box(
        name="Box 1",
        id=1,
        price=100,
        description="some description",
    )
    response = await service.create_box(box=box)
    response = await service.get_boxes()
    print(response)

    # don't forget to close the channel when done!
    channel.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
