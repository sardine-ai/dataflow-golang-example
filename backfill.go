package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"reflect"
)

var (
	startDate = flag.String("start_date", "2021-01-01", "Start date of backfill (Inclusive)")
	endDate   = flag.String("end_date", "2021-01-30", "End date of backfill (Exclusive)")
	env       = flag.String("environment", "local", "Environment to run this job")

	processError = beam.NewCounter("process", "error")
	processOk    = beam.NewCounter("process", "ok")
	// will be initialized within updateDatabaseFn.startBundle
	database *gorm.DB
)

func init() {
	beam.RegisterDoFn(reflect.TypeOf((*updateDatabaseFn)(nil)).Elem())
}

type SomeData struct {
	MyField    string `bigquery:"my_field"`
}

func queryData(s beam.Scope, project string) beam.PCollection {
	return bigqueryio.Query(s,
		project,
		fmt.Sprintf(`
			SELECT 
				my_field
			FROM somedb.domedata
			WHERE DATE(timestamp) >= '%s' 
			AND DATE(timestamp) <= '%s' 
		`, *startDate, *endDate),
		reflect.TypeOf(SomeData{}),
		func(options *bigqueryio.QueryOptions) error {
			options.UseStandardSQL = true
			return nil
		},
	)
}

type updateDatabaseFn struct {
	Env     string `json:"env"`
	Project string `json:"project"`
}

func (f *updateDatabaseFn) StartBundle(_ctx context.Context) error {
	var err error
	database, err = initializeDatabase(context.Background(), f.Env, f.Project)
	if err != nil {
		return errors.Wrap(err, "failed to connect to database")
	}
	return nil
}

func (f *updateDatabaseFn) ProcessElement(ctx context.Context, data SomeData) {
	err := insertRecords(database, data)
	if err != nil {
		log.Errorf(ctx, "error updating database %s", err)
		processError.Inc(ctx, 1)
		return
	}
	processOk.Inc(ctx, 1)
	return
}

// dataflow job that replays request from request_log and execute logic to parse payload and insert data to DB
func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()

	project := gcpopts.GetProject(ctx)
	log.Infof(ctx, "running backfill for %s from %s to %s in %s", project, *startDate, *endDate, *env)

	p := beam.NewPipeline()
	s := p.Root()
	data := queryData(s, project)
	beam.ParDo0(s, &updateDatabaseFn{Env: *env, Project: project}, data)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}

func initializeDatabase(ctx context.Context, env string, project string) (*gorm.DB, error){
	log.Fatal(ctx, "implement me!")
	return nil, nil
}

func insertRecords(db *gorm.DB, data SomeData) error {
	log.Fatal(context.Background(), "implement me!")
	return nil
}