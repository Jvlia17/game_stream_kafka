import random
import time
import json
from datetime import datetime, timezone

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


locations = [
    "Village",
    "Forest",
    "Desert",
    "Mountain",
    "Cave",
    "Swamp",
    "Beach",
    "Ruins",
    "Castle",
    "Valley",
    "Lake",
    "Volcano",
    "Plains",
    "Dungeon",
    "Island"
]


items = [
    "Sword",
    "Armor",
    "Rare Gem",
    "Red Potion",
    "Blue Potion"
]


players = {
    "player_1001": {
        "level": 0,
        "status": "online",
        "location": "Village",
        "enemies_killed": 0,
        "items": 0
    },
    "player_1002": {
        "level": 0,
        "status": "online",
        "location": "Forest",
        "enemies_killed": 0,
        "items": 0
    },
    "player_1003": {
        "level": 0,
        "status": "online",
        "location": "Desert",
        "enemies_killed": 0,
        "items": 0
    },
    "player_1004": {
        "level": 0,
        "status": "online",
        "location": "Mountain",
        "enemies_killed": 0,
        "items": 0
    },
    "player_1005": {
        "level": 0,
        "status": "online",
        "location": "Cave",
        "enemies_killed": 0,
        "items": 0
    }
}


def create_event(player_id, event_type, extra_data=None):

    player = players[player_id]

    event = {
        "player_id": player_id,
        "event_type": event_type,
        "level": player["level"],
        "location": player["location"],
        "items": player["items"],
        "enemies_killed": player["enemies_killed"],
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds")
    }

    if extra_data:
        event.update(extra_data)

    return event


def send_event(player_id, event_type, extra_data=None):

    event = create_event(
        player_id,
        event_type,
        extra_data
    )

    producer.send(
        "game-events",
        event
    )

    producer.flush()

    print(f"Sent event: {event}")


def find_pvp_opponent(player_id):

    player = players[player_id]

    opponents = [
        other_id
        for other_id, other_player in players.items()
        if (
            other_id != player_id
            and other_player["status"] == "online"
            and other_player["location"] == player["location"]
        )
    ]

    if opponents:
        return random.choice(opponents)

    return None


def calculate_win_probability(player_id, opponent_id):

    player = players[player_id]
    opponent = players[opponent_id]

    player_power = (
        player["level"] * 10
        + player["items"] * 3
    )

    opponent_power = (
        opponent["level"] * 10
        + opponent["items"] * 3
    )

    total_power = player_power + opponent_power

    if total_power == 0:
        return 0.5

    return player_power / total_power


print("Game Simulator started")
print(f"Players online: {len(players)}")


while True:

    online_players = [
        player_id
        for player_id, player_data in players.items()
        if player_data["status"] == "online"
    ]

    if not online_players:

        print("No players online.")

        time.sleep(4)

        continue

    player_id = random.choice(online_players)

    player = players[player_id]

    opponent_id = find_pvp_opponent(player_id)

    if opponent_id and random.random() < 0.20:

        opponent = players[opponent_id]

        print(
            f"PVP: {player_id} vs {opponent_id} "
            f"in {player['location']}"
        )

        send_event(
            player_id,
            "pvp_started",
            {
                "opponent_id": opponent_id
            }
        )

        win_probability = calculate_win_probability(
            player_id,
            opponent_id
        )

        if random.random() < win_probability:

            winner_id = player_id
            loser_id = opponent_id

        else:

            winner_id = opponent_id
            loser_id = player_id

        send_event(
            winner_id,
            "pvp_won",
            {
                "opponent_id": loser_id
            }
        )

        previous_level = players[loser_id]["level"]

        if random.random() < 0.10:

            new_level = previous_level

            print(
                f"CHEAT: {loser_id} protected level "
                f"{previous_level} after losing to {winner_id}"
            )

        else:

            players[loser_id]["level"] = max(
                0,
                players[loser_id]["level"] - 1
            )

            players[loser_id]["enemies_killed"] = 0

            new_level = players[loser_id]["level"]

        send_event(
            loser_id,
            "player_defeated",
            {
                "opponent_id": winner_id,
                "previous_level": previous_level,
                "new_level": new_level
            }
        )

        print(
            f"PVP RESULT: {winner_id} defeated {loser_id} "
            f"(level {previous_level} -> {new_level})"
        )

    elif random.random() < 0.25:

        new_location = random.choice(locations)

        if new_location != player["location"]:

            old_location = player["location"]

            player["location"] = new_location

            send_event(
                player_id,
                "location_changed",
                {
                    "from_location": old_location,
                    "to_location": new_location
                }
            )

    elif random.random() < 0.02:

        previous_level = player["level"]

        player["level"] += 1

        send_event(
            player_id,
            "cheat_level_up",
            {
                "previous_level": previous_level,
                "new_level": player["level"]
            }
        )

        print(
            f"CHEAT: {player_id} "
            f"{previous_level} -> {player['level']} "
            f"with {player['enemies_killed']} enemies killed"
        )

    else:

        event = random.choices(
            [
                "enemy_killed",
                "item_collected"
            ],
            weights=[
                75,
                25
            ]
        )[0]

        if event == "enemy_killed":

            player["enemies_killed"] += 1

            send_event(
                player_id,
                "enemy_killed"
            )

            if player["enemies_killed"] >= 2:

                previous_level = player["level"]

                player["level"] += 1

                player["enemies_killed"] = 0

                send_event(
                    player_id,
                    "level_completed",
                    {
                        "previous_level": previous_level,
                        "new_level": player["level"]
                    }
                )

                print(
                    f"LEVEL UP: {player_id} "
                    f"{previous_level} -> {player['level']}"
                )

        elif event == "item_collected":

            item = random.choice(items)

            player["items"] += 1

            send_event(
                player_id,
                "item_collected",
                {
                    "item": item
                }
            )

            if random.random() < 0.05:

                second_item = random.choice(items)

                player["items"] += 1

                send_event(
                    player_id,
                    "item_collected",
                    {
                        "item": second_item
                    }
                )

                print(
                    f"DOUBLE LOOT: {player_id} "
                    f"found {item} and {second_item} "
                    f"in {player['location']}"
                )

    time.sleep(2)