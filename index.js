const PORT = require('./config').PORT
const express = require('express');
const app = express();
const redis = require("redis")
const OperationType = [103, 101, 102, 104, 100];
const OrderType = [1, 2];

//Redis Client connections
const redisclient = redis.createClient()
redisclient.on('error', err => console.log('Redis Client Error', err));
redisclient.connect();

//Middlewares
const checkOperationType = (req, res, next) => {
    const body = req.body
    if (OperationType.includes(Number(body.OperationType)) != true) {
        return res.send("Invalid request operation type");
    }

    next();
}

const CheckPacketType = (req, res, next) => {
    const body = req.body
    if (Number(body.MsgType) == 1120 || Number(body.MsgType) == 1121) {
        next();
    }
    else {
        return res.send("Invalid request message type");
    }
}

app.use(express.urlencoded({ extended: false }));
app.use(CheckPacketType);
app.use(checkOperationType);

//Listening on the port 3000 weather server is running or not
app.use(express.json());
app.listen(PORT, () => {
    console.log(`server is running on port ${PORT}`)
})

//Functions for Order Packet
function addOrder(body, req, res) {
    redisclient.HSET(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`, `${body.OrderId}`, JSON.stringify(body));
    return res.send("Order added successfully");
}

function UpdateOrder(body, req, res) {
    redisclient.hExists(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`, `${body.OrderId}`)
        .then((reply) => {
            if (reply) {
                redisclient.HSET(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`, `${body.OrderId}`, JSON.stringify(body));
                res.send("Order updated successfully");
            }
            else {
                res.send("Order not found");
            }
        }).catch((err) => {
            console.log(err);
            res.send("Order not found");
        })

}

function deleteOrder(body, req, res) {
    redisclient.hExists(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`, `${body.OrderId}`)
        .then((reply) => {
            if (reply) {
                redisclient.hDel(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`, `${body.OrderId}`);
                res.send("Order deleted successfully");
            }
            else {
                res.send("Order not found");
            }
        }).catch((err) => {
            console.log(err);
            res.send("Order not found");
        })
}

function getOrder(body, req, res) {
    redisclient.hExists(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`, `${body.OrderId}`).then((reply) => {
        if (reply) {
            redisclient.hGet(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`, `${body.OrderId}`).then
                (value => {
                    return res.send(value);
                })
                .catch(err => {
                    console.log(err);
                    res.send("Order not found");
                })
        }
        else {
            res.send("Order not found");
        }
    }).catch((err) => {
        console.log(err);
        res.send("Order not found");
    })
}

function getAllOrders(body, req, res) {
    redisclient.hGetAll(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`).then
        (value => {
            return res.send(value);
        })
        .catch(err => {
            console.log(err);
            res.send("Orders not found");
        })
}


//Main Function handling Orders
function handleOrderRequest(body, req, res) {
    console.log(body.OperationType);
    if (OrderType.includes(Number(body.OrderType)) != true) {
        return res.send("Invalid request order type");
    }
    console.log(`${body.TenantId}_${body.OMSId}_${body.ClientId}_${body.Token}`);
    switch (Number(body.OperationType)) {
        //add
        case 100:
            addOrder(body, req, res)
            break;
        //update
        case 101:
            {
                UpdateOrder(body, req, res);
                break;
            }
        case 102:
            {
                deleteOrder(body, req, res)
                break;
            }
        //get
        case 103:
            {
                getOrder(body, req, res)
                break;
            }
        //getall
        case 104:
            {
                getAllOrders(body, req, res)
                break;
            }

    }

}

//Functions handling clients
function AddClient(body, req, res) {
    redisclient.hSet(`${body.TenantId}_${body.OMSId}`, `${body.ClientId}`, JSON.stringify(body));
    return res.send("Client added successfully");
}

function UpdateClient(body, req, res) {
    redisclient.hExists(`${body.TenantId}_${body.OMSId}`, `${body.ClientId}`).then((reply) => {
        console.log(reply);
        if (reply) {
            redisclient.hSet(`${body.TenantId}_${body.OMSId}`, `${body.ClientId}`, JSON.stringify(body));
            res.send("Client updated successfully");
        }
        else if (reply == 0) {
            res.send("Client not found");
        }
    }).catch((err) => {
        console.log(err);
        res.send("Client not found in hashset");
    })
}

function DeleteClient(body, req, res) {
    redisclient.hExists(`${body.TenantId}_${body.OMSId}`, `${body.ClientId}`).then((reply) => {
        if (reply) {
            redisclient.hDel(`${body.TenantId}_${body.OMSId}`, `${body.ClientId}`);
            res.send("Client deleted successfully");
        }
        else {
            res.send("Client not found");
        }
    })
}

function GetClient(body, req, res) {
    redisclient.hExists(`${body.TenantId}_${body.OMSId}`, `${body.ClientId}`).then((reply) => {
        if (reply) {
            redisclient.hGet(`${body.TenantId}_${body.OMSId}`, `${body.ClientId}`).then
                (value => {
                    return res.send(value);
                })
                .catch(err => {
                    console.log(err);
                    res.send("Client not found");
                })
        }
        else {
            res.send("Client not found");
        }
    }).catch((err) => {
        console.log(err);
        res.send("Client not found");
    })
}

function getAllClients(body, req, res) {
    redisclient.hGetAll(`${body.TenantId}_${body.OMSId}`).then(reply => {
        if (Object.keys(reply).length != 0) {
            return res.send(reply);
        }
        else if (Object.keys(reply).length == 0) {
            res.send("No clients found mod");
        }
    }).catch(err => {
        console.log(err);
        res.send("No clients found");
    })
}

//Main Function Handling Clients
function handleClientRequest(body, req, res) {
    console.log("Currently in client request");
    switch (Number(body.OperationType)) {
        //add
        case 100:
            {
            AddClient(body, req, res);
            break;
            }
        //update
        case 101:
            {
            UpdateClient(body, req, res);
            break;
            }
        //delete
        case 102:
            {
            DeleteClient(body, req, res);
            break;
            }
        //get
        case 103:
            {
            GetClient(body, req, res);
            break;
            }
        //getall
        case 104:
            {
                getAllClients(body, req, res);
                break;
            }
    }
}



app.route('/').get((req, res) => {
    // console.log(req.body);
    const body = (req.body);

    if (Number(body.MsgType) == 1120) {
        handleOrderRequest(body, req, res);
    }
    else if (Number(body.MsgType) == 1121) {
        handleClientRequest(body, req, res);
    }

})