var cursor = db.getCollection('tank_stats').aggregate([
//     {$match: {tank_id: 7169}},
    {$match: {total_battles: {$gt: 0}}},
    {$group: {
        "_id": "$account_id",
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