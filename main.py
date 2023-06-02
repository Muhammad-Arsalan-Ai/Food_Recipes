import requests
from fastapi import FastAPI
app = FastAPI()
@app.get("/recipes")
def get_recipes():
    url = "https://tasty.p.rapidapi.com/recipes/list"
    querystring = {"from": "0", "size": "20", "tags": "under_30_minutes"}
    headers = {
        "X-RapidAPI-Key": "43f261cfb8msh07b4a9375ae71bap1da682jsn95323af568fd",
        "X-RapidAPI-Host": "tasty.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()















