FROM python:3.10

WORKDIR analytics

COPY .. /analytics

RUN python -m venv venv
RUN pip install -r requirements.txt

CMD ["python", "bootstrap.py"]