FROM python:3-slim
MAINTAINER Timur Samkharadze "timur.samkharadze@sysco.no"

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++

COPY ./service /service
WORKDIR /service
RUN pip install -r requirements.txt

RUN  apt-get purge -y --auto-remove gcc g++ \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["service.py"]