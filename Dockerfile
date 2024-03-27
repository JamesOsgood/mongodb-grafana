FROM node:20.12.0

WORKDIR /app

COPY package.json package-lock.json ./

RUN npm ci

COPY . .

EXPOSE 3333

CMD ["npm", "run", "server"]
