# GameStream — Real-Time Event Streaming & Cheat Detection

**Apache Kafka | Flink | Docker | Amazon MSK | Amazon EC2 | Amazon S3 | AWS Glue | AWS Lambda | Athena | Amazon SNS**

A streaming data pipeline for near-real-time cheat detection and cost-efficient gameplay analytics.


# Business Problem

A gaming company continuously streams gameplay logs and needs to **detect suspicious player behavior in near real time**, automatically notifying its team when potential cheating is detected. It also needs to **store raw logs** cost-effectively, optimize them for analytical queries, and support **historical analysis** using SQL.

---

# My approach

To simulate and tackle this problem, I have used many different AWS services. 

- For streaming game logs, I used an **Amazon EC2** instance to host the producer. It runs a producer.py script that generates gameplay events based on the rules described in the Gameplay section.
- Apache Kafka (Amazon MSK)
- Amazon S3
- Apache Flink
- AWS Lambda and Amazon SNS
- AWS Glue
- Amazon Athena

---

# Implementation



---

# Gameplay

The gameplay is inspired by Metin2.

Five players interact across 15 locations. Each player can:
- **Travel:** Move to another location (25% chance per turn).
- **Fight enemies:** Kill mobs and gain one level after every two kills (75% chance when staying).
- **Collect items:** Search the current location for items (25% chance when staying).
- **Fight other players:** When two players meet at the same location, they may engage in PvP. The outcome depends on their levels, items, and luck. The loser normally loses one level.

For entertainment purposes and to keep our anti-cheat system busy, a few players occasionally bend the rules:
- **Illegal level-up:** 2% chance of gaining a level without killing two mobs.
- **Level protection cheat:** 10% chance of keeping a level after losing a PvP fight.
- **Double item drop:** 5% chance of receiving two items from a single loot roll.

---

# Project Structure




# How to Run the Pipeline


