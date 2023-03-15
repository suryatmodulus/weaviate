//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

func groupByObjects(t *testing.T) {
	t.Run("group by: people by city", func(t *testing.T) {
		query := `
		{
			Get{
				Person(
					nearObject:{
						id: "8615585a-2960-482d-b19d-8bee98ade52c" 
					}
					groupBy:{
						path:["livesIn"]
						groups:4
						objectsPerGroup: 10
					}
				){
					name
					_additional{
						id
						group{
							count
							maxDistance
							minDistance
							hits {
								content
								_additional{
									distance
								}
							}
						}
					}
				}
			}
		}
		`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		groups := result.Get("Get", "Person").AsSlice()

		require.Len(t, groups, 3)

		getGroup := func(value interface{}) map[string]interface{} {
			group := value.(map[string]interface{})["_additional"].(map[string]interface{})["group"].(map[string]interface{})
			return group
		}

		getGroupHits := func(group map[string]interface{}) []string {
			result := []string{}
			hits := group["hits"].([]interface{})
			for _, hit := range hits {
				content := hit.(map[string]interface{})["content"]
				result = append(result, content.(string))
			}
			return result
		}

		expectedGroup1 := []string{
			"8615585a-2960-482d-b19d-8bee98ade52c",
			"3ef44474-b5e5-455d-91dc-d917b5b76165",
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}

		expectedGroup2 := []string{
			"3ef44474-b5e5-455d-91dc-d917b5b76165",
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}

		expectedGroup3 := []string{
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}

		for _, current := range groups {
			group := getGroup(current)
			ids := getGroupHits(group)
			if len(ids) == 3 {
				assert.ElementsMatch(t, expectedGroup1, ids)
			} else if len(ids) == 2 {
				assert.ElementsMatch(t, expectedGroup2, ids)
			} else if len(ids) == 1 {
				assert.ElementsMatch(t, expectedGroup3, ids)
			}
		}
	})
}
