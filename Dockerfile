FROM rust

WORKDIR /app

COPY . .

CMD ["cargo", "run"]