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
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

func groupByObjects(t *testing.T) {
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

	t.Run("group by: passages by documents", func(t *testing.T) {
		create := func(t *testing.T, multishard bool) {
			for _, class := range documents.ClassesContextionaryVectorizer(multishard) {
				createObjectClass(t, class)
			}
			for _, obj := range documents.Objects() {
				helper.CreateObject(t, obj)
				helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
			}
		}
		groupBy := func(t *testing.T) {
			query := `
			{
				Get{
					Passage(
						nearObject:{
							id: "00000000-0000-0000-0000-000000000001" 
						}
						groupBy:{
							path:["ofDocument"]
							groups:2
							objectsPerGroup: 10
						}
					){
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
			groups := result.Get("Get", "Passage").AsSlice()

			require.Len(t, groups, 3)

			expectedGroup1 := []string{
				documents.PassageIDs[0].String(),
				documents.PassageIDs[1].String(),
				documents.PassageIDs[2].String(),
				documents.PassageIDs[3].String(),
				documents.PassageIDs[4].String(),
				documents.PassageIDs[5].String(),
			}

			expectedGroup2 := []string{
				documents.PassageIDs[6].String(),
				documents.PassageIDs[7].String(),
			}

			for _, current := range groups {
				group := getGroup(current)
				ids := getGroupHits(group)
				if len(ids) == 3 {
					assert.ElementsMatch(t, expectedGroup1, ids)
				} else if len(ids) == 2 {
					assert.ElementsMatch(t, expectedGroup2, ids)
				}
			}
		}
		delete := func(t *testing.T) {
			deleteObjectClass(t, documents.Passage)
			deleteObjectClass(t, documents.Document)
		}

		tests := []struct {
			name       string
			multishard bool
		}{
			{
				name:       "single shard",
				multishard: false,
			},
			{
				name:       "multi shard",
				multishard: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Run("create", func(t *testing.T) {
					create(t, tt.multishard)
				})
				t.Run("group by", func(t *testing.T) {
					groupBy(t)
				})
				t.Run("delete", func(t *testing.T) {
					delete(t)
				})
			})
		}
	})
}
