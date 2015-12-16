FROM ubuntu:14.04

# Install system dependencies
RUN apt-get install -y python
RUN apt-get install -y python-pip

# Install Python dependencies
ADD . /requirements.txt
RUN pip install -r requirements.txt
RUN pip install .

# Copy app files
ADD . /lbc-crawler.py

#CMD myapp --port 8000
