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
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type IndexstatsCollector struct {
	Ctx             context.Context
	Client          *mongo.Client
	Collections     []string
	DiscoveringMode bool
	Logger          *logrus.Logger
	TopologyInfo    LabelsGetter
}

func (d *IndexstatsCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

func (d *IndexstatsCollector) Collect(ch chan<- prometheus.Metric) {
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
			{Key: "$indexStats", Value: bson.M{}},
		}

		cursor, err := d.Client.Database(database).Collection(collection).Aggregate(d.Ctx, mongo.Pipeline{aggregation})
		if err != nil {
			d.Logger.Errorf("cannot get $indexStats cursor for collection %s.%s: %s", database, collection, err)
			continue
		}

		var stats []bson.M
		if err = cursor.All(d.Ctx, &stats); err != nil {
			d.Logger.Errorf("cannot get $indexStats for collection %s.%s: %s", database, collection, err)
			continue
		}

		d.Logger.Debugf("indexStats for %s.%s", database, collection)
		debugResult(d.Logger, stats)

		for _, m := range stats {
			// prefix and labels are needed to avoid duplicated metric names since the metrics are the
			// same, for different collections.
			prefix := fmt.Sprintf("%s_%s_%s", database, collection, m["name"])
			labels := d.TopologyInfo.baseLabels()
			labels["namespace"] = database + "." + collection
			labels["key_name"] = fmt.Sprintf("%s", m["name"])

			metrics := sanitizeMetrics(m)
			for _, metric := range makeMetrics(prefix, metrics, labels, false) {
				ch <- metric
			}
		}
	}
}

// According to specs, we should expose only this 2 metrics. 'building' might not exist.
func sanitizeMetrics(m bson.M) bson.M {
	ops := float64(0)

	if val := walkTo(m, []string{"accesses", "ops"}); val != nil {
		if f, err := asFloat64(val); err == nil {
			ops = *f
		}
	}

	filteredMetrics := bson.M{
		"accesses": bson.M{
			"ops": ops,
		},
	}

	if val := walkTo(m, []string{"building"}); val != nil {
		if f, err := asFloat64(val); err == nil {
			filteredMetrics["building"] = *f
		}
	}

	return filteredMetrics
}

var _ prometheus.Collector = (*IndexstatsCollector)(nil)
