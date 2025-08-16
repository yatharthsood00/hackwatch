# HackWatch

A Python script to track GeekHack.

## Installation

1. Install dependencies, run main.py. Script will create ```hackwatch.db``` in your root folder.
2. Whenever re-running, the script will keep running unless it finds an row that doesn't need modifications, at which point it will stop running and move on to the next board.
3. Currently runs on boards "Interest Checks" and "Group Buys and Preorders" only.

**Future plans** - Working with *shudder* the Discord API to create the HackWatch Bot, as Geekwatch is no longer possible to set up on a server.