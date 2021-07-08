<?php

namespace App\Importer;

use App\Entity\ClientImporter;
use App\Entity\FieldConfiguration;
use App\Model\ImportCriteria;
use App\Model\SourceField;
use DateTime;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Query\QueryBuilder;
use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use MathParser\Exceptions\DivisionByZeroException;
use MathParser\Interpreting\Evaluator;
use MathParser\Parsing\Nodes\FunctionNode;
use MathParser\StdMathParser;

class BaseBigQueryImporter extends BaseImporter
{
   private ?Connection $connection = null;
   private ?BigQueryClient $client = null;

   protected string $tablePrefix = '';
   protected string $clientKeyField = 'companyName';
   protected string $dateField = 'date';
   protected ?string $dateFieldSelect = 'date';
   protected string $asinField = 'asin';
   protected ?string $asinFieldSelect = 'asin';
   protected string $rootTableAlias = 'root';

   /**
    * @throws Exception
    */
   public function getConnection(): Connection
   {
       if (null != $this->connection) {
           return $this->connection;
       }
       /*Empty connection to use doctrine to build queries and then use them in big query*/
       $this->connection = DriverManager::getConnection([
//            'url' => $_ENV['DATABASE_URL'],
           'url' => getenv('DATABASE_URL'),
           'driver' => 'pdo_pgsql',
       ]);

       return $this->connection;
   }

   public function getClient(): ?BigQueryClient
   {
       if ($this->client) {
           return $this->client;
       }

       return $this->client = new BigQueryClient([
           /*Temporary username on bigquery is the project id and host holds the keyFile content*/
           'projectId' => $this->importerConfig->getUsername(),
           'keyFile' => json_decode($this->importerConfig->getHost(), true),
           'scopes' => [BigQueryClient::SCOPE],
       ]);
   }

   /**
    * @return ImportedValue[]|array
    *
    * @throws Exception
    */
   protected function executeQuery(QueryBuilder $query, FieldConfiguration $fieldConfiguration): ?array
   {
       $importValues = [];
       $parser = new StdMathParser();
       /** @var FunctionNode $AST */
       $AST = $parser->parse($fieldConfiguration->getCalculationString());
       $evaluator = new Evaluator();
       $client = $this->getClient();
       $queryJobConfig = $client->query($query->getSQL(), [
           'useQueryCache' => true,
       ]);
       $queryResults = $client->runQuery($queryJobConfig);
       $results = $queryResults->rows();
       $count = 0;
       foreach ($results as $result) {
           ++$count;
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
           if (null !== $value && 0.0 != $value) {
               $importValue->setValue($value)
                   ->setTimestamp(new DateTime($result[$this->dateField]))
                   ->setAsin($result[$this->asinField]);
               $importValues[] = $importValue;
           }
       }

       return $importValues;
   }
}
