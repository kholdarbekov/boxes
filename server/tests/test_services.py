import asyncio
import pytest
from datetime import datetime, timedelta
from pymongo import MongoClient

from server.server import DatabaseService
from server.db import Box, RequestStatus
from server.db_manager import get_database


def get_test_database():
    # Create a MongoClient connection using default localhost:27017
    client = MongoClient()
    # Create the database and return it
    boxes_db = client.boxes
    boxes_indexes_dict = boxes_db.boxes.index_information()
    if "category_index" not in boxes_indexes_dict:
        boxes_db.boxes.create_index("category", name="category_index")

    if "created_at_index" not in boxes_indexes_dict:
        boxes_db.boxes.create_index("created_at", name="created_at_index")
    return boxes_db, client


@pytest.fixture
def box_service():
    boxes_db, mongo_client = get_test_database()
    service = DatabaseService(boxes_db=boxes_db)
    yield service
    mongo_client.drop_database(boxes_db)


@pytest.mark.asyncio
async def test_get_boxes(box_service):
    # when there is no boxes in DB
    # get_boxes will return empty list
    response = await box_service.get_boxes()
    assert response.status == RequestStatus.OK
    assert response.box == []

    # create some boxes first
    box1 = Box(name='Box1', id=1)
    box2 = Box(name='Box2', id=2)
    response = await box_service.create_box(box=box1)
    assert response.status == RequestStatus.OK
    response = await box_service.create_box(box=box2)
    assert response.status == RequestStatus.OK

    response = await box_service.get_boxes()
    assert response.status == RequestStatus.OK
    for box in response.box:
        assert box.id in (box1.id, box2.id)
    
    #clean up
    response = await box_service.delete_box(id=box1.id)
    assert response.status == RequestStatus.OK
    response = await box_service.delete_box(id=box2.id)
    assert response.status == RequestStatus.OK


@pytest.mark.asyncio
async def test_get_box(box_service):
    response = await box_service.delete_box(id=1)
    # create a box first
    box1 = Box(name='Box1', id=1)
    response = await box_service.create_box(box=box1)
    assert response.status == RequestStatus.OK

    response = await box_service.get_box(id=box1.id)
    assert response.status == RequestStatus.OK
    assert response.box.name == box1.name
    assert response.box.id == box1.id
    
    # if we send non existing id to get_box 
    # it will return status as ERROR
    response = await box_service.get_box(id=9999999)
    assert response.status == RequestStatus.ERROR
    assert response.box is None

    #clean up
    response = await box_service.delete_box(id=box1.id)
    assert response.status == RequestStatus.OK


@pytest.mark.asyncio
async def test_create_box(box_service):
    box1 = Box(name='Box1', id=1)
    response = await box_service.create_box(box=box1)
    assert response.status == RequestStatus.OK
    # id is unique field in our DB, 
    # it will give DuplicatKeyError upon same id creation
    box_duplicate = Box(name='Box1 new', id=1)
    response = await box_service.create_box(box=box_duplicate)
    assert response.status == RequestStatus.ERROR
    
    #clean up
    response = await box_service.delete_box(id=box1.id)
    assert response.status == RequestStatus.OK


@pytest.mark.asyncio
async def test_update_box(box_service):
    # create a box first
    box1 = Box(name='Box1', id=1)
    response = await box_service.create_box(box=box1)
    assert response.status == RequestStatus.OK
    
    box1.name = "Box1 new"
    response = await box_service.update_box(box=box1)
    assert response.status == RequestStatus.OK

    box1.name = "Box1 new"
    box1.id = 999999
    response = await box_service.update_box(box=box1)
    assert response.status == RequestStatus.ERROR
    
    #clean up
    box1.id = 1
    response = await box_service.delete_box(id=box1.id)
    assert response.status == RequestStatus.OK


@pytest.mark.asyncio
async def test_delete_box(box_service):
    # create a box first
    box1 = Box(name='Box1', id=1)
    response = await box_service.create_box(box=box1)
    assert response.status == RequestStatus.OK
    
    response = await box_service.delete_box(id=999999)
    assert response.status == RequestStatus.ERROR

    response = await box_service.delete_box(id=box1.id)
    assert response.status == RequestStatus.OK


@pytest.mark.asyncio
async def test_get_boxes_in_category(box_service):
    # when there is no boxes in DB
    # get_boxes_in_category will return empty list
    response = await box_service.get_boxes_in_category(category="NON_EXISTING_CATEGORY")
    assert response.status == RequestStatus.OK
    assert response.box == []

    # create some boxes first
    box1 = Box(name='Box1', id=1, category="TEST_CATEGORY_1")
    box2 = Box(name='Box2', id=2, category="TEST_CATEGORY_1")
    box3 = Box(name='Box3', id=3, category="TEST_CATEGORY_2")
    box4 = Box(name='Box4', id=4) # Default, empty string as category
    response = await box_service.create_box(box=box1)
    assert response.status == RequestStatus.OK
    response = await box_service.create_box(box=box2)
    assert response.status == RequestStatus.OK
    response = await box_service.create_box(box=box3)
    assert response.status == RequestStatus.OK
    response = await box_service.create_box(box=box4)
    assert response.status == RequestStatus.OK

    response = await box_service.get_boxes_in_category(category="TEST_CATEGORY_1")
    assert response.status == RequestStatus.OK
    assert len(response.box) == 2
    for box in response.box:
        assert box.id in (box1.id, box2.id)

    response = await box_service.get_boxes_in_category(category="TEST_CATEGORY_2")
    assert response.status == RequestStatus.OK
    assert len(response.box) == 1
    assert response.box[0].id == box3.id
    
    response = await box_service.get_boxes_in_category(category='')
    assert response.status == RequestStatus.OK
    assert len(response.box) == 1
    assert response.box[0].id == box4.id
    
    #clean up
    response = await box_service.delete_box(id=box1.id)
    assert response.status == RequestStatus.OK
    response = await box_service.delete_box(id=box2.id)
    assert response.status == RequestStatus.OK
    response = await box_service.delete_box(id=box3.id)
    assert response.status == RequestStatus.OK
    response = await box_service.delete_box(id=box4.id)
    assert response.status == RequestStatus.OK


@pytest.mark.asyncio
async def test_get_boxes_in_time_range(box_service):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=1)
    
    # when there is no boxes in DB
    # get_boxes_in_time_range will return empty list
    response = await box_service.get_boxes_in_time_range(start_time=start_time, end_time=end_time)
    assert response.status == RequestStatus.OK
    assert response.box == []

    # create some boxes first
    box1 = Box(name='Box1', id=1, category="TEST_CATEGORY_1")
    box2 = Box(name='Box2', id=2, category="TEST_CATEGORY_1")
    box3 = Box(name='Box3', id=3, category="TEST_CATEGORY_2")
    box4 = Box(name='Box4', id=4) # Default, empty string as category
    response = await box_service.create_box(box=box1)
    assert response.status == RequestStatus.OK
    response = await box_service.create_box(box=box2)
    assert response.status == RequestStatus.OK

    # fix end_time right after creating above two boxes
    # we should get these two boxes in this new range
    end_time = datetime.utcnow()

    # create these two boxes after 3 secs
    await asyncio.sleep(3)
    response = await box_service.create_box(box=box3)
    assert response.status == RequestStatus.OK
    response = await box_service.create_box(box=box4)
    assert response.status == RequestStatus.OK
    
    response = await box_service.get_boxes_in_time_range(start_time=start_time, end_time=end_time)
    assert response.status == RequestStatus.OK
    assert len(response.box) == 2
    for box in response.box:
        assert box.id in (box1.id, box2.id)
    
    # fix end_time right again to include box3 and box4
    # also change start time to exclude box1 and box2
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(seconds=2)

    response = await box_service.get_boxes_in_time_range(start_time=start_time, end_time=end_time)
    assert response.status == RequestStatus.OK
    assert len(response.box) == 2
    for box in response.box:
        assert box.id in (box3.id, box4.id)

    #clean up
    response = await box_service.delete_box(id=box1.id)
    assert response.status == RequestStatus.OK
    response = await box_service.delete_box(id=box2.id)
    assert response.status == RequestStatus.OK
    response = await box_service.delete_box(id=box3.id)
    assert response.status == RequestStatus.OK
    response = await box_service.delete_box(id=box4.id)
    assert response.status == RequestStatus.OK
