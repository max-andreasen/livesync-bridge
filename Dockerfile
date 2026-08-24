FROM denoland/deno:2.4.5

WORKDIR /app

VOLUME /app/dat
VOLUME /app/data

COPY . .

RUN deno install --allow-import

CMD [ "deno", "task", "run" ]
