package estuary

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/filecoin-project/bacalhau/pkg/ipfs/car"
	"github.com/filecoin-project/bacalhau/pkg/job"
	"github.com/filecoin-project/bacalhau/pkg/model"
	"github.com/filecoin-project/bacalhau/pkg/publisher"
	"github.com/filecoin-project/bacalhau/pkg/system"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type EstuaryPublisherConfig struct {
	APIKey string
}

type EstuaryPublisher struct {
	Config EstuaryPublisherConfig
}

func NewEstuaryPublisher(
	ctx context.Context,
	cm *system.CleanupManager,
	config EstuaryPublisherConfig,
) (*EstuaryPublisher, error) {
	if config.APIKey == "" {
		return nil, fmt.Errorf("APIKey is required")
	}

	log.Ctx(ctx).Debug().Msgf("Estuary publisher initialized")
	return &EstuaryPublisher{
		Config: config,
	}, nil
}

func (estuaryPublisher *EstuaryPublisher) IsInstalled(ctx context.Context) (bool, error) {
	_, span := newSpan(ctx, "IsInstalled")
	defer span.End()
	client, err := GetGatewayClient(ctx, estuaryPublisher.Config)
	if err != nil {
		return false, err
	}
	response, err := client.GetCollectionsWithResponse(ctx)
	return response.StatusCode() == http.StatusOK, err
}

func (estuaryPublisher *EstuaryPublisher) PublishShardResult(
	ctx context.Context,
	shard model.JobShard,
	hostID string,
	shardResultPath string,
) (model.StorageSpec, error) {
	ctx, span := newSpan(ctx, "PublishShardResult")
	defer span.End()

	log.Ctx(ctx).Info().Msgf("Publishing shard %v results to Estuary", shard)
	tempDir, err := os.MkdirTemp("", "bacalhau-estuary-publisher")
	if err != nil {
		return model.StorageSpec{}, err
	}
	carFile := filepath.Join(tempDir, "results.car")
	cid, err := car.CreateCar(ctx, shardResultPath, carFile, 1)
	if err != nil {
		return model.StorageSpec{}, err
	}

	shuttles, err := GetShuttleClients(ctx, estuaryPublisher.Config)
	if err != nil {
		return model.StorageSpec{}, err
	}

	// Try each host until one succeeds.
	for _, client := range shuttles {
		fileReader, err := os.Open(carFile)
		if err != nil {
			return model.StorageSpec{}, err
		}
		defer fileReader.Close()

		_, err = client.PostContentAddCarWithBodyWithResponse(ctx, &PostContentAddCarParams{}, "multipart/form-data", fileReader)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msgf("failed to upload to Estuary host")
			continue
		} else {
			return job.GetPublishedStorageSpec(shard, model.StorageSourceEstuary, hostID, cid), nil
		}
	}

	return model.StorageSpec{}, fmt.Errorf("failed to upload to any Estuary host")
}

func newSpan(ctx context.Context, apiName string) (context.Context, trace.Span) {
	return system.Span(ctx, "publisher/estuary", apiName)
}

// Compile-time check that Verifier implements the correct interface:
var _ publisher.Publisher = (*EstuaryPublisher)(nil)
