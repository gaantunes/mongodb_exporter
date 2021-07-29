// mongodb_exporter
// Copyright (C) 2017 Percona LLC
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package exporter

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type CollstatsCollector struct {
	Ctx             context.Context
	Client          *mongo.Client
	Collections     []string
	CompatibleMode  bool
	DiscoveringMode bool
	Logger          *logrus.Logger
	TopologyInfo    LabelsGetter
}

func (d *CollstatsCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

func (d *CollstatsCollector) Collect(ch chan<- prometheus.Metric) {
	if d.DiscoveringMode {
		databases := map[string][]string{}
		for _, dbCollection := range d.Collections {
			parts := strings.Split(dbCollection, ".")
			if _, ok := databases[parts[0]]; !ok {
				db := parts[0]
				databases[db], _ = d.Client.Database(parts[0]).ListCollectionNames(d.Ctx, bson.D{})
			}
		}

		d.Collections = fromMapToSlice(databases)
	}
	for _, dbCollection := range d.Collections {
		parts := strings.Split(dbCollection, ".")
		if len(parts) != 2 { //nolint:gomnd
			continue
		}

		database := parts[0]
		collection := parts[1]

		aggregation := bson.D{
			{
				Key: "$collStats", Value: bson.M{
					"latencyStats": bson.E{Key: "histograms", Value: true},
					"storageStats": bson.E{Key: "scale", Value: 1},
				},
			},
		}
		project := bson.D{
			{
				Key: "$project", Value: bson.M{
					"storageStats.wiredTiger":   0,
					"storageStats.indexDetails": 0,
				},
			},
		}

		cursor, err := d.Client.Database(database).Collection(collection).Aggregate(d.Ctx, mongo.Pipeline{aggregation, project})
		if err != nil {
			d.Logger.Errorf("cannot get $collstats cursor for collection %s.%s: %s", database, collection, err)
			continue
		}

		var stats []bson.M
		if err = cursor.All(d.Ctx, &stats); err != nil {
			d.Logger.Errorf("cannot get $collstats for collection %s.%s: %s", database, collection, err)
			continue
		}

		d.Logger.Debugf("$collStats metrics for %s.%s", database, collection)
		debugResult(d.Logger, stats)

		// Since all collections will have the same fields, we need to use a metric prefix (db+col)
		// to differentiate metrics between collection. Labels are being set only to matke it easier
		// to filter
		prefix := database + "." + collection

		labels := d.TopologyInfo.baseLabels()
		labels["database"] = database
		labels["collection"] = collection

		for _, metrics := range stats {
			for _, metric := range makeMetrics(prefix, metrics, labels, d.CompatibleMode) {
				ch <- metric
			}
		}
	}
}

func fromMapToSlice(databases map[string][]string) []string {
	var collections []string
	for db, cols := range databases {
		for _, value := range cols {
			collections = append(collections, db+"."+value)
		}
	}

	return collections
}

var _ prometheus.Collector = (*CollstatsCollector)(nil)
