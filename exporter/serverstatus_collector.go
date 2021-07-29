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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type ServerStatusCollector struct {
	Ctx            context.Context
	Client         *mongo.Client
	CompatibleMode bool
	Logger         *logrus.Logger
	TopologyInfo   labelsGetter
}

func (d *ServerStatusCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

func (d *ServerStatusCollector) Collect(ch chan<- prometheus.Metric) {
	cmd := bson.D{{Key: "serverStatus", Value: "1"}}
	res := d.Client.Database("admin").RunCommand(d.Ctx, cmd)

	var m bson.M
	if err := res.Decode(&m); err != nil {
		ch <- prometheus.NewInvalidMetric(prometheus.NewInvalidDesc(err), err)
		return
	}

	logrus.Debug("serverStatus result:")
	debugResult(d.Logger, m)

	for _, metric := range makeMetrics("", m, d.TopologyInfo.baseLabels(), d.CompatibleMode) {
		ch <- metric
	}
}
