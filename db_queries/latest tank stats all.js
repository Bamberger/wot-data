var cursor = db.getCollection('tank_stats').aggregate([
//     {$match: {tank_id: 7169}},
    {$match: {
        region: "sea",
        total_battles: {$gt: 0},
//         tank_id: 46849
        tank_id: { $in: [46849,61697,8497,63537,5937,21761,55841,16961,58369,19985,58641,2721,3473,15905,20993,17217,19473,19201,2433,15617,19489,15425,6193,4737,5425,20225,17729,13905,3937,19969,4145,9233,12369,22017,8705,2417,13889,3681,11841,15953,6209,6225,14113,13857,14609,13569,12305,15697,7249,8481,9297,6145,16913,6929,13089,13825,12049,19217,16897,10785,3649,14881,9489,7169]}
    }},
    {$group: {
        "_id": {
            "account_id": "$account_id",
            "tank_id": "$tank_id"
        },
        "maxLB": {
            $max: "$last_battle_time"
        },
        "docs": {
            $push: {
                "_id": "$_id",
                "account_id": "$account_id",
                "last_battle_time": "$last_battle_time",
                "tank_id": "$tank_id",
                "total_battles": "$total_battles",
                "region": "$region",
                "mark_of_mastery": "$mark_of_mastery",
                "random": "$random"
            }
        }
    }},
    {$project: {
        "maxLB": 1,
        "docs": {
            $setDifference: [{
                    $map: {
                        "input": "$docs",
                        "as": "doc",
                        "in": {
                            "$cond": [{
                                    $eq: ["$maxLB", "$$doc.last_battle_time"]
                                },
                                "$$doc",
                                false
                            ]
                        }
                    }
                },
                [false]
            ]
        }
    }
}, {
    $project: {
        _id: 1,
        doc: {
            $arrayElemAt: ["$docs", 0]
        }
    }
}, {
    $project: {
        _id: "$doc._id",
        account_id: "$doc.account_id",
        last_battle_time: "$doc.last_battle_time",
        tank_id: "$doc.tank_id",
        total_battles: "$doc.total_battles",
        region: "$doc.region",
        mark_of_mastery: "$doc.mark_of_mastery",
        random: "$doc.random"
    }
}, {
    $group: {
        _id: "$tank_id",
        accounts: {
            $sum: 1
        },
        random_battles: {
            $sum: "$random.battles"
        },
        random_wins: {
            $sum: "$random.wins"
        },
        random_dmg: {
            $sum: "$random.damage_dealt"
        },
        random_frg: {
            $sum: "$random.frags"
        },
        random_spo: {
            $sum: "$random.spotted"
        },
        random_def: {
            $sum: "$random.dropped_capture_points"
        }
    }
}, {
    $project: {
        _id: 1,
        random_battles: 1,
        accounts: 1,
        random_dmg_avg: {
            $divide: ["$random_dmg", "$random_battles"]
        },
        random_frg_avg: {
            $divide: ["$random_frg", "$random_battles"]
        },
        random_spo_avg: {
            $divide: ["$random_spo", "$random_battles"]
        },
        random_def_avg: {
            $divide: ["$random_def", "$random_battles"]
        },
        random_win_avg: {
            $divide: ["$random_wins", "$random_battles"]
        }
    }
}],{ allowDiskUse: true });
     
while(cursor.hasNext()) {
    print(tojson(cursor.next()))
}