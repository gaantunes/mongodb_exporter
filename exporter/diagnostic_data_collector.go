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

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type DiagnosticDataCollector struct {
	Ctx            context.Context
	Client         *mongo.Client
	CompatibleMode bool
	Logger         *logrus.Logger
	TopologyInfo   labelsGetter
}

func (d *DiagnosticDataCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

func (d *DiagnosticDataCollector) Collect(ch chan<- prometheus.Metric) {
	var m bson.M

	cmd := bson.D{{Key: "getDiagnosticData", Value: "1"}}
	res := d.Client.Database("admin").RunCommand(d.Ctx, cmd)

	if err := res.Decode(&m); err != nil {
		d.Logger.Errorf("cannot run getDiagnosticData: %s", err)

		return
	}

	m, ok := m["data"].(bson.M)
	if !ok {
		err := errors.Wrapf(errUnexpectedDataType, "%T for data field", m["data"])
		d.Logger.Errorf("cannot decode getDiagnosticData: %s", err)

		return
	}

	d.Logger.Debug("getDiagnosticData result")
	debugResult(d.Logger, m)

	metrics := makeMetrics("", m, d.TopologyInfo.baseLabels(), d.CompatibleMode)
	metrics = append(metrics, locksMetrics(m)...)

	if d.CompatibleMode {
		metrics = append(metrics, specialMetrics(d.Ctx, d.Client, m, d.Logger)...)

		if cem, err := cacheEvictedTotalMetric(m); err == nil {
			metrics = append(metrics, cem)
		}

		nodeType, err := getNodeType(d.Ctx, d.Client)
		if err != nil {
			d.Logger.Errorf("Cannot get node type to check if this is a mongos: %s", err)
		} else if nodeType == typeMongos {
			metrics = append(metrics, mongosMetrics(d.Ctx, d.Client, d.Logger)...)
		}
	}

	for _, metric := range metrics {
		ch <- metric
	}
}

// check interface.
var _ prometheus.Collector = (*DiagnosticDataCollector)(nil)
