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


def main():
    db_conn = sqlite3.connect('feed.db')
    init_db(db_conn)
    work_item_tracking_client = init_ado()
    db_cursor = db_conn.cursor()

    # 4 weeks
    days_to_include = 4 * 7
    start_datetime = datetime.datetime.today() - datetime.timedelta(days=days_to_include)
    print(f"Start Date: {start_datetime}")

    feed_data = feedparser.parse(feed_url)

    for index, item in enumerate(feed_data.entries):
        published_datetime = datetime.datetime.fromtimestamp(time.mktime(item.published_parsed))

        if published_datetime < start_datetime:
            continue

        # See if item exists
        db_cursor.execute("SELECT COUNT(*) FROM items WHERE guid = ?", (item.id, ))

        db_count_result = db_cursor.fetchone()
        db_count = db_count_result[0]

        # Skip if item exists
        if db_count > 0:
            continue

        db_cursor.execute("INSERT INTO items (guid, timestamp) VALUES (?, ?)",
                          (item.id, published_datetime.timestamp(), ))
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

        document = []

        set_field(document, "/fields/System.Title",         f"{item.title}")
        set_field(document, "/fields/System.AreaPath",      azure_devops_area_path)

        set_field(
            document,
            "/fields/System.Description",
            f"{item.description}<br />\n<br />\n<a href=\"{item.link}\">Source</a>"
        )

        set_field(document, "/fields/System.Tags",          azure_devops_tags)

        set_field(document, "/relations/-", WorkItemRelation(
            rel="System.LinkTypes.Hierarchy-Reverse",
            url=azure_devops_epic_url,
            attributes={
                "name": "Parent",
            }
        ))

        user_story_response = work_item_tracking_client.create_work_item(
            document, azure_devops_project, "User Story")

        print("User Story Response:")
        print(user_story_response)
        print("")
        print(f"User Story ID: {user_story_response.id}")
        print("")
        print("")

    db_conn.close()


main()
