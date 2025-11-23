# ELK Stack Docker

## Create new repos folder and navigate to directory
```console
cd ~/repos/elk-docker
```

## Clone Docker Repo
git clone https://github.com/stacktrekker/docker-elk.git

## Navigate to directory and run docker setup. Then compose
```console
cd docker-elk
docker compose up setup
docker compose up
```

## Download Movies Data
```bash
curl -L -o movies250_v3.json https://raw.githubusercontent.com/stacktrekker/docker-elk-files/refs/heads/main/movies250_v3.json
```
## Create Movie Index
```json
{
  "mappings": {
    "properties": {
      "Title": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "Year": { 
        "type": "integer" 
      },
      "Rated": { 
        "type": "keyword" 
      },
      "Released": { 
        "type": "date", 
        "format": "yyyy-MM-dd" 
      },
      "Runtime": { 
        "type": "integer" 
      },
      "Genre": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "Director": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "Writer": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "Actors": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "Language": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "Country": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "Awards": { 
        "type": "text" 
      },
      "imdbRating": { 
        "type": "float" 
      },
      "imdbID": { 
        "type": "keyword" 
      },
      "Type": { 
        "type": "keyword" 
      }
    }
  }
}
```

## Load to Elastic Search as Binary file using PUT verb
POST {{baseURL}}/movies/_bulk
Body: Binary
File: movies250_v3.json

## Full Search Collection
Full Postman requests are under the "ElasticSearch" collection
