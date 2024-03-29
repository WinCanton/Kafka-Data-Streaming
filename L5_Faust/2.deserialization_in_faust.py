from dataclasses import asdict, dataclass
import json

import faust


#
# DONE: Define a ClickEvent Record Class with an email (str), timestamp (str), uri(str),
#       and number (int)
#
#       See: https://docs.python.org/3/library/dataclasses.html
#       See: https://faust.readthedocs.io/en/latest/userguide/models.html#model-types
#
@dataclass
class ClickEvent(faust.Record, validation=True, serializer="json"):
    email: str = ""
    timestamp: str = ""
    uri: str = ""
    number: int = 0

app = faust.App("exercise2", broker="kafka://localhost:9092")

#
# DONE: Provide the key (uri) and value type to the clickevent
#
clickevents_topic = app.topic(
    "com.udacity.streams.clickevents",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents:
        print(json.dumps(asdict(ce), indent=10))


if __name__ == "__main__":
    app.main()
