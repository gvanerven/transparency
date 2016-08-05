function getItensLicitacoes(licitacao, offset, max) {
	var request = require("request");
	var licitacoes = "http://compras.dados.gov.br/licitacoes/v1/licitacoes.json?uasg=" + licitacao + "&offset=" + offset;
	var mongodb = require('mongodb');
	var MongoClient = mongodb.MongoClient;
	var urlMongo = 'mongodb://localhost:27017/comprasnet';	
	request({
		url: licitacoes,
		json: true
	}, function (error, response, body) {

		if (!error && response.statusCode === 200) {
			console.log(body._embedded.licitacoes.length);
			MongoClient.connect(urlMongo, function (err, db) {
			  if (err) {
				console.log('Unable to connect to the mongoDB server. Error:', err);
			  } else {
					//HURRAY!! We are connected. :)
					console.log('Connection established to', urlMongo);
					var collection = db.collection('licitacoes');
					// do some work here with the database.
					collection.insert(body._embedded.licitacoes, {w: 1}, function (err, result) {
						if (err) {
							console.log(err);
						} else {
							console.log('Inserted %d documents into the "licitacoes" collection.', result.length);
						}
					});
					//Close connection
					db.close();
			  }
			});
			if(max == 0) max = body.count;
			offset = offset + body._embedded.licitacoes.length;
			//console.log(body._embedded.licitacoes.length);
			if(offset < max) getItensLicitacoes(licitacao, offset, max);			
		}else{
			console.log("Error loading page")
		}
	});
}

//getLicitacoes(153229, 0, 0);

var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var ObjectId = require('mongodb').ObjectID;
var url = 'mongodb://localhost:27017/comprasnet';

var getLicitacoesUasg = function(db, callback) {
   var cursor =db.collection('uasgs').find({"id": 10020});
   cursor.each(function(err, uasg) {
      assert.equal(err, null);
      if (uasg != null) {
         getItensLicitacoes(uasg.id, 0, 0);
      } else {
         callback();
      }
   });
};

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  getLicitacoesUasg(db, function() {
      db.close();
  });
});