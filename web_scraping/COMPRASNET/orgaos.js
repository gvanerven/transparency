function getOrgaos(offset, max) {
	var request = require("request");
	var orgaos = "http://compras.dados.gov.br/licitacoes/v1/orgaos.json?offset=" + offset;
	var mongodb = require('mongodb');
	var MongoClient = mongodb.MongoClient;
	var urlMongo = 'mongodb://localhost:27017/comprasnet';	
	request({
		url: orgaos,
		json: true
	}, function (error, response, body) {

		if (!error && response.statusCode === 200) {
			MongoClient.connect(urlMongo, function (err, db) {
			  if (err) {
				console.log('Unable to connect to the mongoDB server. Error:', err);
			  } else {
				//HURRAY!! We are connected. :)
				console.log('Connection established to', urlMongo);
				var collection = db.collection('orgaos');
				// do some work here with the database.
				collection.insert(body._embedded.Orgaos, {w: 1}, function (err, result) {
				  if (err) {
					console.log(err);
				  } else {
					console.log('Inserted %d documents into the "orgaos" collection.', result.length);
				  }
				});
				//Close connection
				db.close();
			  }
			});
			if(max == 0) max = body.count;
			offset = offset + body._embedded.Orgaos.length;
			//console.log(body._embedded.Orgaos.length);
			if(offset < max) getOrgaos(offset, max);			
		}
	});
}

getOrgaos(0, 0);