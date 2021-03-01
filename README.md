# RSS To Azure DevOps

This project parses an RSS feed and pushes new items to Azure DevOps as user stories. As items are processed, a record is added to a sqlite database called `feed.db` that stores the GUID from the RSS feed item and the timestamp that it was processed. This allows you to rerun the program as often as desired without causing duplicate user stories in Azure DevOps.

## Environment Variables:

| Variable | Type | Description |
| -------- | ---- | ----------- |
| `FEED_URL` | `string` | URL of the RSS feed to parse |
| `AZURE_DEVOPS_PAT` | `string` | Personal Access Token (PAT) for the Azure DevOps account |
| `AZURE_DEVOPS_URL` | `string` | Azure DevOps Server URL |
| `AZURE_DEVOPS_PROJECT` | `string` | Azure DevOps Project Name |
| `AZURE_DEVOPS_EPIC_URL` | `string` | URL of the epic to serve as the parent for the user stories |
| `AZURE_DEVOPS_AREA_PATH` | `string` | Area Path in Azure DevOps to place the user stories |
| `AZURE_DEVOPS_TAGS` | `string` | Colon-separated list of tags to add to the user stories |
| `DB_PATH` | `string` | Local path to the database storing scraped items |

### Example:

```env
FEED_URL="https://azurecomcdn.azureedge.net/en-us/updates/feed/"
AZURE_DEVOPS_PAT="4uy5oj38pte27h8gwnqkz9bvx6ct9d8l7xjn1glu6s0j58s6127e"
AZURE_DEVOPS_URL="https://my-org.visualstudio.com/"
AZURE_DEVOPS_PROJECT="MyProject"
AZURE_DEVOPS_EPIC_URL="https://my-org.visualstudio.com/e2956432-c542-408c-bd60-3c4da2bb802a/_apis/wit/workItems/8675309"
AZURE_DEVOPS_AREA_PATH="Top Level\Secondary Level"
AZURE_DEVOPS_TAGS="tag1; tag2; tag3"
DB_PATH="/opt/rss_scraper/feed.db"
```
