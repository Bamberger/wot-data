db.getCollection('account_info').aggregate([
//     {$match: { clan_id: { $exists:true } } },
//     {$match: { clan_id: 2000004634 } },
    { $group:{ 
        "_id": "$account_id",
        "maxLB": { $max: "$last_battle_time" },
        "docs": { $push: {
            "_id": "$_id",
            "account_id": "$account_id",
            "last_battle_time": "$last_battle_time",
            "nickname": "$nickname",
            "global_rating": "$global_rating",
            "clan_id": "$clan_id",
            "statistics": "$statistics"
        }}
    }},
    { $project: {
        "maxLB": 1,
        "docs": {
            $setDifference: [
               { $map: {
                   "input": "$docs",
                   "as": "doc",
                   "in": {
                       "$cond": [ 
                           { $eq: [ "$maxLB", "$$doc.last_battle_time" ] },
                           "$$doc",
                           false
                       ]
                   }
               }},
               [false]
            ]
        }
    }},
    {$project: {
         _id: 1,
         doc: { $arrayElemAt: [ "$docs", 0 ] }
     }},
     {$group:
        {
         _id: "$doc.clan_id",
        random_battles: { $sum: "$doc.statistics.random.battles"},
        random_wins: { $sum: "$doc.statistics.random.wins"},
        random_dmg: { $sum: "$doc.statistics.random.damage_dealt"},
        random_frg: { $sum: "$doc.statistics.random.frags"},
        random_spo: { $sum: "$doc.statistics.random.spotted"},
        random_def: { $sum: "$doc.statistics.random.dropped_capture_points"}
        
        }
     },
//      {$project: {
//          _id: 1,
//          statistics: "$doc.statistics"
//      }},
     
])