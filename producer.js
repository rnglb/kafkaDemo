//const Kafka = require("kafkajs").Kafka
const {Kafka} = require("kafkajs")

const bookdata = require('./book.json');
console.log(bookdata.length);

run();
async function run(){
    try
    {
         const kafka = new Kafka({
              "clientId": "my-kafka-app",
              "brokers" :["192.168.0.116:9092"]
         })

        const producer = kafka.producer();
        console.log("Connecting.....")
        await producer.connect()
        console.log("Connected!")
        //storing Year wise book details
        for(i=0;i<bookdata.length;i++){
        const partition = bookdata[i].year < 0 ? 0 :(bookdata[i].year < 1900 ? 1 : 2);
        const msg = "The book '" + bookdata[i].title + "' is written by " + bookdata[i].author +". It's written in "+bookdata[i].language +" and has published in "+ bookdata[i].country+ "country";
        await producer.send({
            "topic": "Data",
            "messages": [
                {
                    "value": msg,
                    "partition": partition
                }
            ]
        })
    }
 //       console.log(`Send Successfully! ${JSON.stringify(result)}`)
        await producer.disconnect();
    }
    catch(ex)
    {
        console.error(`Something bad happened ${ex}`)
    }
    finally{
        process.exit(0);
    }
}