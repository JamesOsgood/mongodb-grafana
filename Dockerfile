FROM node:carbon

WORKDIR /usr/src/app

COPY . .
RUN npm install

EXPOSE 3333
CMD ["npm", "run", "server"]
