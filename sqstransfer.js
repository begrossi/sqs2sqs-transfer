var AWS = require('aws-sdk');
AWS.config.loadFromPath('./config.json');
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});
var config=require('./config.json');
var Q=require('q');
var _=require('underscore');


//http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/SQS.html#receiveMessage-property
function getMsgs(_num, _numTransferidas, _numErrors) {
    _num = _num || 0;
    _numTransferidas = _numTransferidas||0;

    return Q.fcall(function(){
        var params = {
          QueueUrl: config.fromQueue, /* required */
          //AttributeNames: ['All'],
          MaxNumberOfMessages: 10,
          VisibilityTimeout: 60, //in seconds
          WaitTimeSeconds: 10
        };
        console.log('getting from '+config.fromQueue);
        return Q.ninvoke(sqs,'receiveMessage', params);
    }).then(function(data){
        var msgs = _.uniq(data.Messages, function(m){return m.MessageId;});
        
        console.log(_num, _numTransferidas, 'Mensagens lidas:',_.size(data.Messages),_.size(msgs),false&&JSON.stringify(data,null,2));
        if(_.isEmpty(msgs))
            return Q.reject('Nenhuma mensagem para ler. Total transferidas: '+_numTransferidas);
            

        var msgsEntries=_.map(msgs, function(msg){
            return {
                Id: msg.MessageId,
                MessageBody: msg.Body,
                ReceiptHandle: msg.ReceiptHandle
            };
        });
        console.log('sending to '+config.toQueue);
        return Q.all([msgsEntries, Q.ninvoke(sqs,'sendMessageBatch',{
            Entries: _.map(msgsEntries,function(e){return _.pick(e,'Id','MessageBody')}),
            QueueUrl: config.toQueue
        })]);
    
    }).spread(function(msgsEntries, data){
        console.log(_num, _numTransferidas, 'Mensagens enviadas:',_.size(data.Successful),'Falhas:',_.size(data.Failed),false&&JSON.stringify(data,null,2));
        
        
        var msgsEntris2Del=_.map(data.Successful, function(msg){
            var m = _.findWhere(msgsEntries, {Id:msg.Id});
            return {
                Id: m.Id,
                ReceiptHandle: m.ReceiptHandle
            };
        });
        
        return Q.ninvoke(sqs, 'deleteMessageBatch', {
            QueueUrl: config.fromQueue,
            Entries: msgsEntris2Del
        });
    }).then(function(data){
        console.log(_num, _numTransferidas, 'Mensagens apagadas',_.size(data.Successful),'Falhas:',_.size(data.Failed),false&&JSON.stringify(data,null,2));
        
        
        if(!_.isEmpty(data.Failed))
            return Q.reject('Falha ao apagar mensagens depois de '+_numTransferidas+' mensagens transferidas',JSON.stringify(data,null,2));
        
        console.log('Lendo mais....');
        
        return getMsgs(_num+1, _numTransferidas+_.size(data.Successful));
    });
}


getMsgs().then(function(){
    console.log('Finished');
}).catch(function(err){
    console.error(err, err.stack);
}).done();
