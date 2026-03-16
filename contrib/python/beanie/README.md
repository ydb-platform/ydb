[![Beanie](https://raw.githubusercontent.com/roman-right/beanie/main/assets/logo/white_bg.svg)](https://github.com/roman-right/beanie)

[![shields badge](https://shields.io/badge/-docs-blue)](https://beanie-odm.dev)
[![pypi](https://img.shields.io/pypi/v/beanie.svg)](https://pypi.python.org/pypi/beanie)

## 📢 Important Update 📢

We are excited to announce that Beanie is transitioning from solo development to a team-based approach! This move will help us enhance the project with new features and more collaborative development.

At this moment we are establishing a board of members that will decide all the future steps of the project. We are looking for contributors and maintainers to join the board.

### Join Us
If you are interested in contributing or want to stay updated, please join our Discord channel. We're looking forward to your ideas and contributions!

[Join our Discord](https://discord.gg/AwwTrbCASP)

Let’s make Beanie better, together!

## Overview

[Beanie](https://github.com/roman-right/beanie) - is an asynchronous Python object-document mapper (ODM) for MongoDB. Data models are based on [Pydantic](https://pydantic-docs.helpmanual.io/).

When using Beanie each database collection has a corresponding `Document` that
is used to interact with that collection. In addition to retrieving data,
Beanie allows you to add, update, or delete documents from the collection as
well.

Beanie saves you time by removing boilerplate code, and it helps you focus on
the parts of your app that actually matter.

Data and schema migrations are supported by Beanie out of the box.

There is a synchronous version of Beanie ODM - [Bunnet](https://github.com/roman-right/bunnet)

## Installation

### PIP

```shell
pip install beanie
```

### Poetry

```shell
poetry add beanie
```

For more installation options (eg: `aws`, `gcp`, `srv` ...) you can look in the [getting started](./docs/getting-started.md#optional-dependencies)

## Example

```python
import asyncio
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel

from beanie import Document, Indexed, init_beanie


class Category(BaseModel):
    name: str
    description: str


class Product(Document):
    name: str                          # You can use normal types just like in pydantic
    description: Optional[str] = None
    price: Indexed(float)              # You can also specify that a field should correspond to an index
    category: Category                 # You can include pydantic models as well


# This is an asynchronous example, so we will access it from an async function
async def example():
    # Beanie uses Motor async client under the hood 
    client = AsyncIOMotorClient("mongodb://user:pass@host:27017")

    # Initialize beanie with the Product document class
    await init_beanie(database=client.db_name, document_models=[Product])

    chocolate = Category(name="Chocolate", description="A preparation of roasted and ground cacao seeds.")
    # Beanie documents work just like pydantic models
    tonybar = Product(name="Tony's", price=5.95, category=chocolate)
    # And can be inserted into the database
    await tonybar.insert() 
    
    # You can find documents with pythonic syntax
    product = await Product.find_one(Product.price < 10)
    
    # And update them
    await product.set({Product.name:"Gold bar"})


if __name__ == "__main__":
    asyncio.run(example())
```

## Links

### Documentation

- **[Doc](https://beanie-odm.dev/)** - Tutorial, API documentation, and development guidelines.

### Example Projects

- **[fastapi-cosmos-beanie](https://github.com/tonybaloney/ants-azure-demos/tree/master/fastapi-cosmos-beanie)** - FastAPI + Beanie ODM + Azure Cosmos Demo Application by [Anthony Shaw](https://github.com/tonybaloney)
- **[fastapi-beanie-jwt](https://github.com/flyinactor91/fastapi-beanie-jwt)** - 
  Sample FastAPI server with JWT auth and Beanie ODM by [Michael duPont](https://github.com/flyinactor91)
- **[Shortify](https://github.com/IHosseini083/Shortify)** - URL shortener RESTful API (FastAPI + Beanie ODM + JWT & OAuth2) by [
Iliya Hosseini](https://github.com/IHosseini083)
- **[LCCN Predictor](https://github.com/baoliay2008/lccn_predictor)** - Leetcode contest rating predictor (FastAPI + Beanie ODM + React) by [L. Bao](https://github.com/baoliay2008)

### Articles

- **[Announcing Beanie - MongoDB ODM](https://dev.to/romanright/announcing-beanie-mongodb-odm-56e)**
- **[Build a Cocktail API with Beanie and MongoDB](https://developer.mongodb.com/article/beanie-odm-fastapi-cocktails/)**
- **[MongoDB indexes with Beanie](https://dev.to/romanright/mongodb-indexes-with-beanie-43e8)**
- **[Beanie Projections. Reducing network and database load.](https://dev.to/romanright/beanie-projections-reducing-network-and-database-load-3bih)**
- **[Beanie 1.0 - Query Builder](https://dev.to/romanright/announcing-beanie-1-0-mongodb-odm-with-query-builder-4mbl)**
- **[Beanie 1.8 - Relations, Cache, Actions and more!](https://dev.to/romanright/announcing-beanie-odm-18-relations-cache-actions-and-more-24ef)**

### Resources

- **[GitHub](https://github.com/roman-right/beanie)** - GitHub page of the
  project
- **[Changelog](https://beanie-odm.dev/changelog)** - list of all
  the valuable changes
- **[Discord](https://discord.gg/AwwTrbCASP)** - ask your questions, share
  ideas or just say `Hello!!`

----
Supported by [JetBrains](https://jb.gg/OpenSource)

[![JetBrains](https://raw.githubusercontent.com/roman-right/beanie/main/assets/logo/jetbrains.svg)](https://jb.gg/OpenSource)
