function getLicitacoes(offset, max) {
	var request = require("request");
	//var licitacoes = "http://compras.dados.gov.br/licitacoes/v1/licitacoes.json?data_publicacao_min=2010-01-01&offset=" + offset;
	var licitacoes = "http://compras.dados.gov.br/licitacoes/v1/licitacoes.json?offset=" + offset;
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
			console.log("Next Offset:" + offset + " Max: " + max);
			if(offset < max) getLicitacoes(offset, max);			
		}else{
			console.log("Error loading page: " + error)
			getLicitacoes(offset, max);
		}
	});
}

getLicitacoes(0, 0);