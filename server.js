const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const request = require("request");
const dotenv = require('dotenv');
dotenv.config();

// msec between loop
const loopMsec = process.env.LOOPTIME;
console.log('Loop time: ' + process.env.LOOPTIME);
// Region - sea, ru, na or eu
// const region = process.env.REGION;
// Mongo Connection URL
const dbUrl = process.env.DBURL;
// Mongo Database Name
const dbName = process.env.DBNAME;
// Create a new MongoClient
const client = new MongoClient(dbUrl, {
	useNewUrlParser: true
});
// Create queue for accounts
var queue_accounts = [];

// WG API details for each region
var config = {};
config.sea = {};
config.sea.api_account_list = 'http://api.worldoftanks.asia/wot/account/list/';
config.sea.api_account_info = 'http://api.worldoftanks.asia/wot/account/info/';
config.sea.api_tanks_stats = 'https://api.worldoftanks.asia/wot/tanks/stats/';
config.sea.api_profile_summary = 'https://worldoftanks.asia/wotup/profile/summary/';

config.na = {};
config.na.api_account_list = 'http://api.worldoftanks.com/wot/account/list/';
config.na.api_account_info = 'http://api.worldoftanks.com/wot/account/info/';
config.na.api_tanks_stats = 'https://api.worldoftanks.com/wot/tanks/stats/';
config.na.api_profile_summary = 'https://worldoftanks.com/wotup/profile/summary/';

config.eu = {};
config.eu.api_account_list = 'http://api.worldoftanks.eu/wot/account/list/';
config.eu.api_account_info = 'http://api.worldoftanks.eu/wot/account/info/';
config.eu.api_tanks_stats = 'https://api.worldoftanks.eu/wot/tanks/stats/';
config.eu.api_profile_summary = 'https://worldoftanks.eu/wotup/profile/summary/';

config.ru = {};
config.ru.api_account_list = 'http://api.worldoftanks.ru/wot/account/list/';
config.ru.api_account_info = 'http://api.worldoftanks.ru/wot/account/info/';
config.ru.api_tanks_stats = 'https://api.worldoftanks.ru/wot/tanks/stats/';
config.ru.api_profile_summary = 'https://worldoftanks.ru/wotup/profile/summary/';



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

	// Once the connection is established, start the main loops
	setInterval(fillQueue, process.env.BATCHTIME)
	setInterval(mainCode, loopMsec);
});

// Main Code
function mainCode() {
	// console.log('Running Loop');
	// const db = client.db(dbName);

	if(queue_accounts.length != 0){
		removeDups(queue_accounts);
		var account = queue_accounts.shift();
		// console.log(account)

		//Get the Account Info from WG API and trim
		getAccountInfo(account['account_id'], account['region'], account['last_battle_time'])
		//Save AccountInfo to S3
		.then((account_info) => saveAccountInfo(account_info))
		// Get Tank Stats from WG API and trim
		.then((last_battle_time) => getTankStats(account['account_id'], account['region'], last_battle_time))
		// Save Tank Stats to S3
		.then((tank_stats) => saveTankStats(tank_stats))
		// Update DB with last_battle_time -> next_update_msec
		.then((last_battle_time) => updateAccounts(account['account_id'], account['region'], last_battle_time))
		// Catch any errors
		.catch(err => console.log("ERROR: " + err));
	}
	else {
		// console.log('DB CHECK - No accounts to update');
		return;
	}


}

function fillQueue() {
	const db = client.db(dbName);
	queue_length = queue_accounts.length;
	console.log('Queue size: ' + queue_length);
	// Find accounts that need checking
	if(queue_length < parseInt(process.env.BATCHSIZE)/2){
		db.collection("accounts")
		.aggregate(
		[
			{ $match:
				{$or:[
					{next_update_msec: { $exists:false } },
					{ next_update_msec: { $lt: Date.now() + parseInt(process.env.BATCHTIME)/2} }
				]}
			},
			{ $sample: { size: parseInt(process.env.BATCHSIZE) } },
			{ $project: { _id: 0, account_id: 1, region: 1, last_battle_time: 1 } }
			]
		)
		.toArray(function(err, accountlist) {
			if (err) throw err;
			if(Object.keys(accountlist).length === 0){
				// console.log('DB CHECK - No accounts to update')
				return;
			}
			for (i in accountlist) {
				// If a last_battle_time exists, check before adding to queue
				if(accountlist[i].hasOwnProperty("last_battle_time")) {
					getBattleCount(accountlist[i])
					.then((account_object) => getProfileSummary(account_object))
				}
				// If there was no last_battle_time but the region exists, add it to the queue
				else if (accountlist[i].hasOwnProperty("region")){
					accountlist[i]['last_battle_time'] = 0;
					queue_accounts.push(accountlist[i]);
				}
				// If we get this far something has gone very wrong and the problem account will stay forever.
				// TODO: Handle this somehow
			};
		})
	}
}

function getBattleCount(account) {
	const db = client.db(dbName);
	// console.log(account['account_id']);
	// console.log(account);
	// Check how many random battles our last snapshot had from DB
	return new Promise(function(resolve, reject) {
		db.collection("account_info")
		.aggregate([
			{$match: {"account_id": account['account_id']}},
			{$project: {"account_id": "$account_id", "random_battles": "$statistics.random.battles", "region": "$region"}},
			{$sort: {random_battles: -1}},
			{$limit: 1}
			])
		.toArray(function(err,account_object) {
			if (err) reject(err);
			if (account_object.length === 0) {
				queue_accounts.push(account);
			}
			else { 
				account_object[0]['last_battle_time'] = account['last_battle_time'];
				resolve(account_object[0]);
			};
		});
	});
}

function getProfileSummary(account_object){
	var region = account_object['region'];
	// Setting URL and headers for request
	var propertiesObject = {
		spa_id: account_object['account_id'],
		language: 'en' // Not sure if this actually does anything
	};
	var options = {
		url: config[region].api_profile_summary,
		qs: propertiesObject
	};
	// Start Promise
	return new Promise(function(resolve, reject) {
		// Get latest profile stats from public API
		request.get(options, function(err, resp, body) {
			if (err) {
				reject(err);
			} else {
				var profile_summary = JSON.parse(body);
				// console.log(profile_summary);
				if(profile_summary['data']['battles_count'] != account_object['random_battles']){
					// console.log('Battle counts do not match');
					// console.log('                  MAKE SURE YOU THIS IS NOT EQ');
					queue_accounts.push(account_object)
					// console.log(queue_accounts)
				}
				else {
					// console.log( '*** NO BATTLES *** ' + account_object['account_id'] + ' ' + account_object['region'] + ' ' + account_object['last_battle_time'])
					updateAccounts(account_object['account_id'], account_object['region'], account_object['last_battle_time'])
				}
			}
		})
	})
}

function getAccountInfo(account_id, region,) {
	// console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' API Request')
	// Setting URL and headers for request
	var propertiesObject = {
		application_id: process.env.WGAPPID,
		account_id: account_id,
		extra: 'statistics.random,statistics.ranked_battles,statistics.globalmap_absolute,statistics.globalmap_champion,statistics.globalmap_middle',
		fields: '-statistics.company,-statistics.team,-statistics.regular_team,-statistics.all,-statistics.historical',
		language: 'en'
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

					// If data is null, delete this account to stop it being run against
					if (account_info['data'][account_id] == null) {
						// console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' NULL DATA');
						deleteAccount(account_id,region);
					}

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
					// console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' Response trimmed')
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
	const db = client.db(dbName);

	db.collection("account_info").updateOne({
		account_id: account_id,
		region: region,
		last_battle_time: last_battle_time,
		}, {
			$set: account_info
		},
		{ upsert: true},
		function(err, data) {
			if (err) {
				reject('ERROR - ACCOUNT INFO ' + account_id + ' region: ' + region + ' DB Update failed: ' + err);
				// reject(err);
			} else {
				// console.log('ACCOUNT INFO ' + account_id + ' region: ' + region + ' DB Updated');
				// resolve(last_battle_time);
			}
		});

	return last_battle_time;

}

function getTankStats(account_id, region, last_battle_time) {
	// console.log('TANK STATS ' + account_id + ' region: ' + region + ' API Request')

  // Setting URL and headers for request
	var propertiesObject = {
		application_id: process.env.WGAPPID,
		account_id: account_id,
		extra: 'ranked,random',
		fields: '-company,-team,-regular_team,-all',
		language: 'en'
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
					// If data is null, delete this account to stop it being run against
					if (tank_stats['data'][account_id] == null) {
						// console.log('TANK STATS ' + account_id + ' region: ' + region + ' NULL DATA');
						deleteAccount(account_id,region);
					}
					for (tank in tank_stats['data'][account_id]) {
						var total_battles =
							tank_stats['data'][account_id][tank]['clan']['battles']
							+ tank_stats['data'][account_id][tank]['stronghold_skirmish']['battles']
							+ tank_stats['data'][account_id][tank]['globalmap']['battles']
							+ tank_stats['data'][account_id][tank]['random']['battles']
							+ tank_stats['data'][account_id][tank]['stronghold_defense']['battles']
							+ tank_stats['data'][account_id][tank]['ranked']['battles'];

						tank_stats['data'][account_id][tank].total_battles = total_battles;
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
        //   console.log('TANK STATS ' + account_id + ' region: ' + region + ' Response trimmed')

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
	const db = client.db(dbName);

	try{
		for (tank in tank_stats.tank_stats){
			var total_battles = tank_stats.tank_stats[tank]['total_battles'];
			tank_stats.tank_stats[tank]['last_battle_time'] = last_battle_time;
			delete tank_stats.tank_stats[tank]['in_garage'];
			delete tank_stats.tank_stats[tank]['frags'];


			db.collection("tank_stats").updateOne({
				account_id: account_id,
				region: region,
				total_battles: total_battles,
				tank_id: tank_stats.tank_stats[tank]['tank_id']
				}, {
					$set: tank_stats.tank_stats[tank]
				},
				{ upsert: true},
				function(err, data) {
					if (err) {
						reject('ERROR - TANK STATS ' + account_id + ' region: ' + region + ' DB Update failed: ' + err);
						// reject(err);
					}
				});	
		}
		// console.log('TANK STATS ' + account_id + ' region: ' + region + ' DB Updated');
	}
	catch(error) {
		reject(error)
	};

	return last_battle_time;

}

function updateAccounts(account_id, region, last_battle_time) {
	const db = client.db(dbName);

	var gap_last_battle = Math.abs(
		Date.now() -
		1000 * last_battle_time
	);

	if (gap_last_battle >= 2419200000) {
		var next_update_msec = 2419200000 + Date.now();
	} else {
		var next_update_msec = 86400000 + Date.now();
	}

	db.collection("accounts").updateOne({
		account_id: account_id,
    	region: region
	}, {
		$set: {
			next_update_msec: next_update_msec,
			last_battle_time: last_battle_time
		}
	}, function(err, data) {
		if (err) {
			reject('ERROR - UPDATE ACCOUNT ' + account_id + ' region: ' + region + ' DB Update failed: ' + err);
		} else {
			console.log('UPDATE ACCOUNT ' + account_id + ' region: ' + region + ' DB Updated');
		}
	});

}

function deleteAccount(account_id,region){
	const db = client.db(dbName);

	db.collection("accounts")
    .deleteOne({
		account_id: account_id,
		region: region
	});
	console.log('ACCOUNT ' + account_id + ' region: ' + region + ' DELETED');
	

}

function removeDups(names) {
	let unique = {};
	names.forEach(function(i) {
	  if(!unique[i]) {
		unique[i] = true;
	  }
	});
	return Object.keys(unique);
  }