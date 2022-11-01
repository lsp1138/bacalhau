package estuary

import (
	"context"
	"net/http"

	"github.com/filecoin-project/bacalhau/pkg/model"
	"github.com/filecoin-project/bacalhau/pkg/publisher"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type EstuaryPinner struct {
	ipfsPublisher publisher.Publisher
	apiClient     ClientWithResponsesInterface
}

func NewEstuaryPinner(ctx context.Context, publisher publisher.Publisher, config EstuaryPublisherConfig) (publisher.Publisher, error) {
	client, err := GetGatewayClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &EstuaryPinner{
		ipfsPublisher: publisher,
		apiClient:     client,
	}, nil
}

// IsInstalled implements publisher.Publisher
func (e *EstuaryPinner) IsInstalled(ctx context.Context) (bool, error) {
	ctx, span := newSpan(ctx, "IsInstalled")
	defer span.End()

	isIpfsInstalled, err := e.ipfsPublisher.IsInstalled(ctx)
	if err != nil || !isIpfsInstalled {
		return isIpfsInstalled, err
	}

	response, err := e.apiClient.GetCollectionsWithResponse(ctx)
	if err != nil {
		return false, err
	}
	return response.StatusCode() == http.StatusOK, nil
}

// PublishShardResult implements publisher.Publisher
func (e *EstuaryPinner) PublishShardResult(
	ctx context.Context,
	shard model.JobShard,
	hostID string,
	shardResultPath string,
) (model.StorageSpec, error) {
	ctx, span := newSpan(ctx, "PublishShardResult")
	defer span.End()

	// Use IPFS to publish the result.
	log.Ctx(ctx).Debug().Msg("Publishing result to IPFS")
	spec, err := e.ipfsPublisher.PublishShardResult(ctx, shard, hostID, shardResultPath)
	if err != nil {
		return spec, err
	}

	// Now pin the CID to Estuary, in a goroutine so this can be slow.
	go func() {
		if spec.CID == "" || spec.Name == "" {
			log.Ctx(ctx).Error().Msgf("Spec %v did not contain a CID or name to pin to Estuary", spec)
		}

		response, err := e.apiClient.PostPinningPinsWithResponse(ctx, PostPinningPinsJSONRequestBody{
			Cid:  spec.CID,
			Name: spec.Name,
		})
		success := response.StatusCode() == http.StatusAccepted && err == nil
		level := map[bool]zerolog.Level{true: zerolog.InfoLevel, false: zerolog.ErrorLevel}[success]
		log.Ctx(ctx).WithLevel(level).
			Err(err).
			Str("CID", spec.CID).
			Str("Name", spec.Name).
			Bool("Success", success).
			Int("ResponseStatusCode", response.StatusCode()).
			Msg("Attempted to pin to Estuary")
	}()

	return spec, nil
}

var _ publisher.Publisher = (*EstuaryPinner)(nil)
