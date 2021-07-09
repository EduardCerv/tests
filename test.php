<?php

namespace App\Importer;

use App\Entity\FieldConfiguration;
use DateTime;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Query\QueryBuilder;
use Google\Cloud\BigQuery\BigQueryClient;
use MathParser\Exceptions\DivisionByZeroException;
use MathParser\Interpreting\Evaluator;
use MathParser\StdMathParser;

class BaseBigQueryImporter extends BaseImporter
{
    private ?Connection $connection = null;
    private ?BigQueryClient $client = null;

    protected string $tablePrefix = '';
    protected string $clientKeyField = 'companyName';
    protected string $dateField = 'date';
    protected string $dateFieldSelect = 'date';
    protected string $asinField = 'asin';
    protected string $asinFieldSelect = 'asin';
    protected string $rootTableAlias = 'root';

    /**
     * @return Connection
     */
    public function getConnection(): Connection
    {
        if (null !== $this->connection) {
            return $this->connection;
        }

        $this->connection = DriverManager::getConnection([
            'host' => getenv('DATABASE_URL'),
            'driver' => 'pdo_pgsql',
        ]);

        return $this->connection;
    }

    /**
     * @return BigQueryClient|null
     */
    public function getClient(): ?BigQueryClient
    {
        if (null !== $this->client) {
            return $this->client;
        }

        return $this->client = new BigQueryClient([
            'projectId' => $this->importerConfig->getUsername(),
            'keyFile' => json_decode($this->importerConfig->getHost(), true),
            'scopes' => [BigQueryClient::SCOPE],
        ]);
    }

    /**
     * @param QueryBuilder $query
     * @param FieldConfiguration $fieldConfiguration
     * @return ImportedValue[]|array
     */
    protected function executeQuery(QueryBuilder $query, FieldConfiguration $fieldConfiguration): array
    {
        $results = $this->getQueryResults($query);

        return $this->getImportedValues($fieldConfiguration, $results);
    }

    /**
     * Get the query result as array
     *
     * @param QueryBuilder $query
     * @return array
     */
    private function getQueryResults(QueryBuilder $query): array
    {
        $client = $this->getClient();
        $queryJobConfig = $client->query($query->getSQL(), [
            'useQueryCache' => true,
        ]);
        $queryResults = $client->runQuery($queryJobConfig);

        return $queryResults->rows();
    }

    /**
     * @param FieldConfiguration $fieldConfiguration
     * @param array $results
     * @return array
     */
    private function getImportedValues(FieldConfiguration $fieldConfiguration, array $results): array
    {
        $importValues = [];
        $parser = new StdMathParser();
        $evaluator = new Evaluator();
        $AST = $parser->parse($fieldConfiguration->getCalculationString());
        foreach ($results as $result) {
            $variables = [];
            foreach ($fieldConfiguration->getSourceFields() as $sourceField) {
                $variables[$sourceField->getLabel()] = $result[$sourceField->getLabel()];
            }
            $evaluator->setVariables($variables);
            $importValue = new ImportedValue();
            $value = null;
            try {
                $value = $AST->accept($evaluator);
            } catch (DivisionByZeroException $e) {
                //TODO Log this maybe
            }
            if (null !== $value && 0.0 !== $value) {
                $importValue->setValue($value)
                    ->setTimestamp(new DateTime($result[$this->dateField]))
                    ->setAsin($result[$this->asinField]);
                $importValues[] = $importValue;
            }
        }

        return $importValue;
    }
}
