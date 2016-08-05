function getUasgs(offset, max) {
	var request = require("request");
	var uasgs = "http://compras.dados.gov.br/licitacoes/v1/uasgs.json?offset=" + offset;
	var mongodb = require('mongodb');
	var MongoClient = mongodb.MongoClient;
	var urlMongo = 'mongodb://localhost:27017/comprasnet';	
	request({
		url: uasgs,
		timeout: 7200000,
		json: true
	}, function (error, response, body) {

		if (!error && response.statusCode === 200) {
			console.log(body._embedded.uasgs.length);
			console.log("Offset: " + offset + " Max: " + max);
			MongoClient.connect(urlMongo, function (err, db) {
			  if (err) {
				console.log('Unable to connect to the mongoDB server. Error:', err);
			  } else {
					console.log('Connection established to', urlMongo);
					var collection = db.collection('uasgs');
					// do some work here with the database.
					collection.insert(body._embedded.uasgs, {w: 1}, function (err, result) {
						if (err) {
							console.log(err);
						} else {
							console.log('Inserted %d documents into the "uasgs" collection.', result.length);
						}
					});
					//Close connection
					db.close();
			  }
			});
			if(max == 0) max = body.count;
			offset = offset + body._embedded.uasgs.length;
			console.log(offset);
			if(offset < max) getUasgs(offset, max);			
		}else{
			console.log("Error request page: " + error)
			getUasgs(offset, max);
		}
	});
}

getUasgs(2000, 0);