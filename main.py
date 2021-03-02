import datetime
import feedparser
import os
import sqlite3
import time
from azure.devops.connection import Connection
from azure.devops.v6_0.work_item_tracking.models import JsonPatchOperation
from azure.devops.v6_0.work_item_tracking.models import WorkItemRelation
from azure.devops.v6_0.work_item_tracking.work_item_tracking_client import WorkItemTrackingClient
from msrest.authentication import BasicAuthentication

# Populate variables from environment variables
feed_url = os.getenv('FEED_URL')
azure_devops_pat = os.getenv('AZURE_DEVOPS_PAT')
azure_devops_url = os.getenv('AZURE_DEVOPS_URL')
azure_devops_project = os.getenv('AZURE_DEVOPS_PROJECT')
azure_devops_epic_url = os.getenv('AZURE_DEVOPS_EPIC_URL')
azure_devops_area_path = os.getenv('AZURE_DEVOPS_AREA_PATH')
azure_devops_tags = os.getenv('AZURE_DEVOPS_TAGS')
db_path = os.getenv('DB_PATH')


def init_ado() -> WorkItemTrackingClient:
    credentials = BasicAuthentication('', azure_devops_pat)
    connection = Connection(base_url=azure_devops_url, creds=credentials)
    work_item_tracking_client = connection.clients.get_work_item_tracking_client()
    return work_item_tracking_client


def init_db(db_conn):
    db_conn.execute("CREATE TABLE IF NOT EXISTS items (guid TEXT PRIMARY KEY, timestamp INTEGER NOT NULL);")


def set_field(document, field, value):
    document.append(JsonPatchOperation(
        from_=None,
        op="add",
        path=field,
        value=value))


def exists_in_db(c: sqlite3.Cursor, guid: str) -> bool:
    c.execute("SELECT COUNT(*) FROM items WHERE guid = ?", (guid,))
    return c.fetchone()[0] > 0


def insert_in_db(c: sqlite3.Cursor, guid: str, timestamp: float):
    c.execute("INSERT INTO items (guid, timestamp) VALUES (?, ?)",
              (guid, timestamp,))


def create_work_item(ado_client: WorkItemTrackingClient, parent_url: str, area_path: str,
                     title: str, desc: str, tags: str, item_type: str = "User Story"):
    document = []
    set_field(document, "/fields/System.Title", title)
    set_field(document, "/fields/System.AreaPath", area_path)
    set_field(document, "/fields/System.Description", desc)
    set_field(document, "/fields/System.Tags", tags)
    set_field(document, "/relations/-", WorkItemRelation(
        rel="System.LinkTypes.Hierarchy-Reverse",
        url=parent_url,
        attributes={
            "name": "Parent",
        }
    ))
    return ado_client.create_work_item(document, azure_devops_project, item_type)


def main():
    db_conn = sqlite3.connect('feed.db')
    init_db(db_conn)
    work_item_tracking_client = init_ado()
    db_cursor = db_conn.cursor()
    # How far back to look for new events
    days_to_include = 2 * 7
    start_datetime = datetime.datetime.today() - datetime.timedelta(days=days_to_include)
    print(f"Start Date: {start_datetime}")
    feed_data = feedparser.parse(feed_url)
    curtime = datetime.datetime.now().strftime('%m/%d/%Y')
    f_resp = create_work_item(ado_client=work_item_tracking_client, parent_url=azure_devops_epic_url,
                              tags=azure_devops_tags, desc=f'Evaluate new Azure features - {curtime}',
                              area_path=azure_devops_area_path, title=f'Evaluate new Azure Features - {curtime}',
                              item_type="Feature")
    feature_url = f_resp.url

    for index, item in enumerate(feed_data.entries):
        published_datetime = datetime.datetime.fromtimestamp(time.mktime(item.published_parsed))
        if published_datetime < start_datetime:
            continue
        elif exists_in_db(c=db_cursor, guid=item.id):
            continue
        try:
            resp = create_work_item(ado_client=work_item_tracking_client, parent_url=feature_url,
                                    area_path=azure_devops_area_path, title=f'{item.title}', tags=azure_devops_tags,
                                    desc=f"{item.description}<br />\n<br />\n<a href=\"{item.link}\">Source</a>")
            print("User Story Response:")
            print(resp)
            print("")
            print(f"User Story ID: {resp.id}")
            print("")
        except Exception as err:
            print(f'Failed to add item {item.title}')
            exit(1)

        insert_in_db(c=db_cursor, guid=item.id, timestamp=published_datetime.timestamp())
        db_conn.commit()

        print(f"Item inserted: {item.title}")
        print(f"Item:    {index}")
        print(f"Title:   {item.title}")
        print(f"Date:    {item.published}")
        print(f"Summary: {item.summary}")
        print(f"Desc:    {item.description}")
        print(f"Link:    {item.link}")
        print(f"GUID:    {item.id}")
        print("")
        print("")

    db_conn.close()


main()
