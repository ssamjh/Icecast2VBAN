## Icecast2VBAN ##

I created this project to take a Icecast stream URL and send it to another local device using VBAN. It takes advantage of ffmpeg and Python.

I want to shout out this repository [TheStaticTurtle/pyVBAN](https://github.com/TheStaticTurtle/pyVBAN) which I've got some of the logic for this project from.


### Installing ###

Clone the repository: `git clone https://github.com/ssamjh/Icecast2VBAN.git`

Switch to the directory: `cd Icecast2VBAN`

Then copy the config and fill out the details: `cp config.ini.example config.ini`

And finally bring up the container with docker compose: `docker compose up -d`