const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const request = require("request");
const AWS = require('aws-sdk');

// msec between loop
const loopMsec = 200;
// Folder in the S3 bucket we are using
const s3FolderAccountInfo = 'account_info'
const s3FolderTankStats = 'tank_stats'
// Region - sea, ru, na or eu
const region = process.env.REGION;
// Mongo Connection URL
const dbUrl = process.env.DBURL;
// Mongo Database Name
const dbName = process.env.DBNAME;
// Create a new MongoClient
const client = new MongoClient(dbUrl, {
	useNewUrlParser: true
});

// Setup S3 access
const s3 = new AWS.S3({
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID,
		secretAccessKey: process.env.SECRET_ACCESS_KEY_ID,
	}
});

// WG API details for each region
var config = {};
config.sea = {};
config.sea.application_id = process.env.WGAPPID;
config.sea.api_account_list = 'http://api.worldoftanks.asia/wot/account/list/';
config.sea.api_account_info = 'http://api.worldoftanks.asia/wot/account/info/';
config.sea.api_tanks_stats = 'https://api.worldoftanks.asia/wot/tanks/stats/';

// Heroku Web Keepalive
setInterval(function() {
  http.get(process.env.URL);
}, 300000); // every 5 minutes (300000)
// Create HTTP listener for Keepalive
var http = require("http");
http.createServer(function(req, res) {
	res.writeHead(200, { "Content-Type": "text/plain" });
	res.write("Server is running!");
	res.end();
})
.listen(process.env.PORT);

// Connect to Mongo
client.connect(function(err) {
	assert.equal(null, err);
	console.log("Connected successfully to server");

	// Once the connection is established, start the main loop
	setInterval(mainLoop, loopMsec);

	// client.close();
});

// Main Loop, Gets 1 account_id from the DB for the specified region
function mainLoop() {
	// console.log('Running Loop');
	const db = client.db(dbName);

	// Run the DB query
    db.collection("accounts")
    .aggregate(
      [
        { $match:
          {$and:[
            {$or:[
              {next_update_msec: { $exists:false } },
              { next_update_msec: { $lt: Date.now() } }
            ]},
            { region: region }
          ]}
        },
        { $sample: { size: 1 } },
        { $project: { _id: 0, account_id: 1, region: 1 } }
        ]
    )
		.toArray(function(err, result) {
      if (err) throw err;
      
      // Post getTankStats chain
      //Get the Account Info from WG API and trim
      getAccountInfo(result[0]['account_id'], region)
      //Save AccountInfo to S3
      .then((account_info) => saveAccountInfo(account_info))
      // Get Tank Stats from WG API and trim
      .then((last_battle_time) => getTankStats(result[0]['account_id'], region, last_battle_time))
      // Save Tank Stats to S3
      .then((tank_stats) => saveTankStats(tank_stats))
      // Update DB with last_battle_time -> next_update_msec
			.then((last_battle_time) => updateAccounts(result[0]['account_id'], region, last_battle_time))
			// Catch any errors
      .catch(err => console.log("ERROR: " + err));

		});
}

function getAccountInfo(account_id, region) {
	console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' API Request')
	// Setting URL and headers for request
	var propertiesObject = {
		application_id: config[region].application_id,
		account_id: account_id
	};
	var options = {
		url: config[region].api_account_info,
		qs: propertiesObject
	};
	// Start Promise
	return new Promise(function(resolve, reject) {
		request.get(options, function(err, resp, body) {
			if (err) {
				reject(err);
			} else {
        // Parse and trim the result, if something goes wrong this stage is caught and next stages will not execute
				try {
					console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' API Response: ' + resp.statusCode)
					var account_info = JSON.parse(body);
					// console.log('***** ORIGINAL *****');
					// console.log(account_info['data'][account_id]['statistics']);
					for (element in account_info['data'][account_id]['statistics']) {
						try {
							if (account_info['data'][account_id]['statistics'][element]['battles'] == 0) {
								delete account_info['data'][account_id]['statistics'][element];
							}
						}
						// Errors are normal
						catch (error) {}
					};
					account_info['data'][account_id]['region'] = region;
					console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' Response trimmed')
					// console.log('***** TRIMMED *****');
					// console.log(account_info['data'][account_id]['statistics']);
					resolve(account_info['data'][account_id]);
				} catch (error) {
					reject(error)
				}
			}
		})
	})
}

function saveAccountInfo(account_info) {
	var account_id = account_info['account_id'];
	var last_battle_time = account_info['last_battle_time'];
	var region = account_info['region'];
	var keystring = s3FolderAccountInfo + '/' + account_id + '-' + last_battle_time

	s3.putObject({
		Bucket: process.env.S3BUCKET,
		Key: keystring,
		Body: JSON.stringify(account_info),
		ContentType: "application/json"
	}, function(err, data) {
		if (err) {
			console.log('ERROR - ACCOUNT INFO ' + account_id + ' region: ' + region + ' Uploading to S3: ' + err);
		} else {
			console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' Uploaded to S3');
		}
	});

	return last_battle_time;

}

function getTankStats(account_id, region, last_battle_time) {
	console.log('TANK STATS ' + account_id + ' region: ' + region + ' API Request')

  // Setting URL and headers for request
	var propertiesObject = {
		application_id: config[region].application_id,
		account_id: account_id,
		extra: 'ranked',
		fields: '-company,-team,-regular_team'
	};
	var options = {
		url: config[region].api_tanks_stats,
		qs: propertiesObject
	};
	// Start Promise
	return new Promise(function(resolve, reject) {
		request.get(options, function(err, resp, body) {
			if (err) {
				reject(err);
			} else {
        // Parse and trim the result, if something goes wrong this stage is caught and next stages will not execute
				try {
					console.log('TANK STATS ' + account_id + ' region: ' + region + ' API Response: ' + resp.statusCode)
					var tank_stats = JSON.parse(body);
					// console.log('***** ORIGINAL *****');
					// console.log(tank_stats['data'][account_id]);
					for (tank in tank_stats['data'][account_id]) {
            for(battle_type in tank_stats['data'][account_id][tank]) {
              try {
                if (tank_stats['data'][account_id][tank][battle_type]['battles'] == 0) {
                  delete tank_stats['data'][account_id][tank][battle_type];
                }
              }
              // Errors are normal
              catch (error) {}
            }
          };

          // console.log('***** TRIMMED *****');
					// console.log(tank_stats['data'][account_id]);
          
          var output = {}
          output['account_id'] = account_id;
          output['region'] = region;
          output['last_battle_time'] = last_battle_time;
          output['tank_stats'] = tank_stats['data'][account_id]
          console.log('TANK STATS ' + account_id + ' region: ' + region + ' Response trimmed')

					resolve(output);
				} catch (error) {
					reject(error)
				}
			}
		})
	})
}

function saveTankStats(tank_stats) {
	var account_id = tank_stats['account_id'];
	var last_battle_time = tank_stats['last_battle_time'];
	var region = tank_stats['region'];
	var keystring = s3FolderTankStats + '/' + account_id + '-' + last_battle_time

	s3.putObject({
		Bucket: process.env.S3BUCKET,
		Key: keystring,
		Body: JSON.stringify(tank_stats),
		ContentType: "application/json"
	}, function(err, data) {
		if (err) {
			console.log('ERROR - TANK STATS ' + account_id + ' region: ' + region + ' Uploading to S3: ' + err);
		} else {
			console.log('TANK STATS ' + account_id + ' region: ' + region + ' Uploaded to S3');
		}
	});

	return last_battle_time;

}

function updateAccounts(account_id, region, last_battle_time) {
	const db = client.db(dbName);

	var gap_last_battle = Math.abs(
		Date.now() -
		1000 * last_battle_time
	);

	if (gap_last_battle >= 604800000) {
		var next_update_msec = 604800000 + Date.now();
		// context.log(next_update_msec)
	} else {
		var next_update_msec = 86400000 + Date.now();
	}

	db.collection("accounts").updateOne({
		account_id: account_id,
    region: region
	}, {
		$set: {
			next_update_msec: next_update_msec
		}
	}, function(err, data) {
		if (err) {
			console.log('ERROR - UPDATE ACCOUNT ' + account_id + ' region: ' + region + ' DB Update failed: ' + err);
		} else {
			console.log('UPDATE ACCOUNT ' + account_id + ' region: ' + region + ' DB Updated');
		}
	});

}